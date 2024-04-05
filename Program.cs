using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Research.Science.Data;
using Microsoft.Research.Science.Data.Imperative;
using Microsoft.Research.Science.Data.NetCDF4;

namespace SerializeNC;
internal class Program
{
    private static void Main(string[] args)
    {
        string ncFilename = args[0];
        string outName = args[1];
        int nVars = args.Length - 2;
        string[] varNames = new string[nVars];
        for (int i = 0; i < nVars; i++)
        {
            varNames[i] = args[i + 2];
        }
        Console.WriteLine($"Beginning serialization of {nVars} variables in {ncFilename}.");
        Console.WriteLine($"Data will be stored in files with name {string.Format(outName,"VARIABLE")}.");
        Console.WriteLine($"Longitudes and latitudes will be stored in {string.Format(outName,"LON1D")} (or LAT1D accordingly).");
        
        // Open the file
        NetCDFUri dsUri = new NetCDFUri
        {
            FileName = ncFilename,
            OpenMode = ResourceOpenMode.ReadOnly
        };

        DataSet ds = DataSet.Open(dsUri);
        long[] longTimes = NetcdfSerializer.ReadFileTimes(ds);
        foreach (string varName in varNames)
        {
            string outFile = string.Format(outName,varName);
            NetcdfSerializer.SerializeVariable(varName, ds, outFile, longTimes);
            
            /*
            // Now read in the data.. is it the same?
            int rank = NetcdfSerializer.GetRank(outFile);
            if (rank == 3)
            {
                (DateTime[] timeVec, float[,,] data) = NetcdfSerializer.ReadFile2D(outFile);
                Console.WriteLine($"Match of {varName} for binary read: {CompareArrays(ds.GetData<float[,,]>(varName),data)}");
            }
            else if (rank == 4)
            {
                (DateTime[] timeVec, float[,,,] data) = NetcdfSerializer.ReadFile3D(outFile);
                float[,,,] ncData = ds.GetData<float[,,,]>(varName);
                Console.WriteLine($"Match of {varName} for binary read: {CompareArrays3D(ncData,data)}");
            }
            */
        }
    }
    public static bool CompareArrays(Array data1, Array data2)
    {
        return Enumerable.Range(0,data1.Rank).All(dimension => data1.GetLength(dimension) == data2.GetLength(dimension)) && 
               data1.Cast<float>().SequenceEqual(data2.Cast<float>());
    }
    
    public static bool CompareArrays3D(float[,,,] data1, float[,,,] data2)
    {
        return Enumerable.Range(0,data1.Rank).All(dimension => data1.GetLength(dimension) == data2.GetLength(dimension)) && 
               data1.Cast<float>().SequenceEqual(data2.Cast<float>());
    }
}