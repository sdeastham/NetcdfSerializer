using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Microsoft.Research.Science.Data;
using Microsoft.Research.Science.Data.Imperative;
using Microsoft.Research.Science.Data.NetCDF4;

namespace SerializeNC;

internal class Program
{
    private static void Main(string[] args)
    {
        string baseDir = args[0];
        DateTime startDate = DateTime.Parse(args[1]);
        DateTime endDate = DateTime.Parse(args[2]);
        Console.WriteLine(
            $"Processing all days between {startDate} and {endDate} in {baseDir} assuming a MERRA-2 file structure.");
        DateTime currDate = startDate;
        string[] fileTypeList = ["A3dyn", "A3cld", "I3"];
        FileProcessor processor = new FileProcessor(runTests: args.Length > 1 && args[1] == "test", internalPrint: false);
        while (currDate < endDate)
        {
            Console.WriteLine($"Processing {currDate}...");
            int year = currDate.Year;
            int month = currDate.Month;
            int day = currDate.Day;
            foreach (string fileType in fileTypeList)
            {
                processor.ProcessFile([$"{year}/{month,2:d2}/MERRA2.{year}{month,2:d2}{day,2:d2}.{fileType}.05x0625.nc4"]);
            }

            currDate += TimeSpan.FromDays(1);
        }
    }
}

public class FileProcessor(bool runTests = false, bool internalPrint = true)
{
    public void ProcessFile(string[] args)
    {
        if (args.Length < 1)
        {
            throw new ArgumentException(
                "Not enough arguments! Must provide input file.");
        }
        string ncFilename = args[0];

        string outName;
        if (args.Length == 1)
        {
            if (ncFilename.Contains(".05x0625"))
            {
                outName = ncFilename.Replace(".05x0625", ".{0}.05x0625");
            }
            else
            {
                throw new ArgumentException("Input file not recognized; explicit output file format needed.");
            }
        }
        else
        {
            outName = args[1];
        }
        
        string[] varNames;
        int nVars = args.Length - 2;
        if (nVars <= 0)
        {
            string shortName = Path.GetFileName(ncFilename);
            if (shortName.Contains("A3dyn"))
            {
                varNames = ["U", "V", "OMEGA"];
            }
            else if (shortName.Contains("A3cld"))
            {
                varNames = ["QI", "QL"];
            }
            else if (shortName.Contains("I3"))
            {
                varNames = ["PS", "QV", "T"];
            }
            else
            {
                throw new ArgumentException("Variable list not provided and file not recognized. Explicit variable list needed.");
            }
            nVars = varNames.Length;
        }
        else
        {
            varNames = new string[nVars];
            for (int i = 0; i < nVars; i++)
            {
                varNames[i] = args[i + 2];
            }
        }

        if (internalPrint)
        {
            Console.WriteLine($"Beginning serialization of {nVars} variables in {ncFilename}.");
            Console.WriteLine($"Data will be stored in files with name {string.Format(outName, "VARIABLE")}.");
            Console.WriteLine(
                $"Longitudes and latitudes will be stored in {string.Format(outName, "LON1D")} (or LAT1D accordingly).");
        }

        // Open the file
        NetCDFUri dsUri = new NetCDFUri
        {
            FileName = ncFilename,
            OpenMode = ResourceOpenMode.ReadOnly
        };

        DataSet ds = DataSet.Open(dsUri);
        string outFile;
        
        long[] longTimes = NetcdfSerializer.ReadFileTimes(ds);
        // Start with latitude and longitude
        outFile = string.Format(outName, "LAT1D");
        NetcdfSerializer.SerializeVariable("lat", ds, outFile, null);
        outFile = string.Format(outName, "LON1D");
        NetcdfSerializer.SerializeVariable("lon", ds, outFile, null);

        Stopwatch writeTimer = new Stopwatch();
        Stopwatch ncReadTimer = new Stopwatch();
        Stopwatch deserializeTimer = new Stopwatch();
        
        foreach (string varName in varNames)
        {
            writeTimer.Start();
            outFile = string.Format(outName,varName);
            NetcdfSerializer.SerializeVariable(varName, ds, outFile, longTimes);
            writeTimer.Start();
            
            // Now read in the data.. is it the same?
            if (!runTests) continue;
            
            deserializeTimer.Start();
            int rank = NetcdfSerializer.GetRank(outFile);
            if (rank == 3)
            {
                (DateTime[] timeVec, float[,,] data) = NetcdfSerializer.Deserialize2D(outFile);
                deserializeTimer.Stop();
                ncReadTimer.Start();
                float[,,] ncData = ds.GetData<float[,,]>(varName);
                ncReadTimer.Stop();
                Console.WriteLine(
                    $"Match of {varName} for binary read: {CompareArrays(ncData, data)}");
            }
            else if (rank == 4)
            {
                (DateTime[] timeVec, float[,,,] data) = NetcdfSerializer.Deserialize3D(outFile);
                deserializeTimer.Stop();
                ncReadTimer.Start();
                float[,,,] ncData = ds.GetData<float[,,,]>(varName);
                ncReadTimer.Stop();
                Console.WriteLine($"Match of {varName} for binary read: {CompareArrays(ncData, data)}");
            }
        }
        
        Console.WriteLine($"Read and serialization completed in {writeTimer.ElapsedMilliseconds/(nVars * 1000.0),10:f2} seconds per variable.");
        if (runTests)
        {
            double ncReadPerVar = ncReadTimer.ElapsedMilliseconds / (nVars * 1000.0);
            double deserializePerVar = deserializeTimer.ElapsedMilliseconds / (nVars * 1000.0);
            Console.WriteLine($"Serial data re-read in {deserializePerVar,10:f2} seconds per variable.");
            Console.WriteLine($"NetCDF      re-read in {ncReadPerVar,10:f2} seconds per variable.");
            Console.WriteLine($"Speedup: {ncReadPerVar/deserializePerVar,10:f2}x ({100.0*(1.0 - deserializePerVar/ncReadPerVar),10:f2}% time saving).");
        }
    }

    public static bool CompareArrays(Array data1, Array data2)
    {
        return Enumerable.Range(0, data1.Rank)
                   .All(dimension => data1.GetLength(dimension) == data2.GetLength(dimension)) &&
               data1.Cast<float>().SequenceEqual(data2.Cast<float>());
    }
}