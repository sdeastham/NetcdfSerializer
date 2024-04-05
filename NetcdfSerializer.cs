using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Research.Science.Data;
using Microsoft.Research.Science.Data.Imperative;
using Microsoft.Research.Science.Data.NetCDF4;
using Microsoft.Research.Science.Data.Utilities;

namespace SerializeNC;

public static class NetcdfSerializer
{
    public static void SerializeVariable(string varName, DataSet ds, string outFile, long[]? fileTimes=null)
    {
        ReadOnlyVariableCollection varList = ds.Variables;
        Variable targVar = varList.First(ncVar => ncVar.Name == varName);
        if (targVar == null)
        {
            throw new ArgumentException($"Variable {varName} not found in dataset {ds.URI}");
        }
        
        // Read in all data for this variable and flatten it into a 1-D array
        int[] varDims = new int[4];
        // How many values will we need to write?
        long longLength = targVar.TotalLength();
        // Verify that we will be able to create a 1D block array with this many bytes..
        if ((longLength*4) > int.MaxValue - 500)
        {
            throw new NotImplementedException($"Too many values ({longLength}) for single-block read/write.");
        }
        int nVals = (int)longLength;
        float[] data1D = new float[nVals];
        int nTimes = fileTimes?.Length ?? 1;
        if (targVar.Rank == 4)
        {
            float[,,,] varData = ds.GetData<float[,,,]>(varName);
            for (int i = 0; i < 4; i++)
            {
                varDims[i] = varData.GetLength(i);
            }
            Flatten(varData, varDims, data1D);
        }
        else if (targVar.Rank == 3)
        {
            float[,,] varData = ds.GetData<float[,,]>(varName);
            varDims[0] = nTimes;
            varDims[1] = 1; // Levels
            // Lat and Lon
            for (int i = 0; i < 2; i++)
            {
                varDims[i + 2] = varData.GetLength(i + 1);
            }
            Flatten(varData, varDims, data1D);
        }
        // Shove everything into a single block of bytes:
        // 1 byte:      Flags [currently just 1: whether the time data is included]
        // 24 bytes:    a 12-character name
        // 16 bytes:    the array dimensions as four integers (first is assumed to be time)
        // T * 8 bytes: T times, as seconds since 1970-01-01T00:00:00Z (long integers) (if times given)
        // N * 4 byes:  N values, as floats
        byte[] flags = CreateFlagsByte(fileTimes != null);
        int flagBytes = 1;
        int nameLength = 12;
        int timeBytes = fileTimes?.Length * 8 ?? 0;
        int nameBytes = 2 * nameLength;
        int dimBytes = 4 * varDims.Length;
        string varShortName = varName.Length > nameLength ? varName.Substring(0, nameLength) : varName.PadRight(nameLength);
        using (var stream = File.Open(outFile, FileMode.Create))
        {
            using (var writer = new BinaryWriter(stream, Encoding.Unicode))
            {
                byte[] bigBlock = new byte[flagBytes + nameBytes + dimBytes + timeBytes + (4*nVals)];
                Buffer.BlockCopy(flags, 0, bigBlock, 0, flagBytes);
                Buffer.BlockCopy(Encoding.Unicode.GetBytes(varShortName),0,bigBlock,flagBytes,nameBytes);
                Buffer.BlockCopy(varDims,0,bigBlock,flagBytes + nameBytes,dimBytes);
                if (timeBytes > 0)
                {
                    Buffer.BlockCopy(fileTimes,0,bigBlock,flagBytes + nameBytes + dimBytes,timeBytes);
                }
                Buffer.BlockCopy(data1D,0,bigBlock,flagBytes + nameBytes + dimBytes + timeBytes,nVals*4);
                writer.Write(bigBlock);
            }
        }
    }

    private static byte[] CreateFlagsByte(bool includesTime)
    {
        byte flagsByte = new byte();
        bool[] flags = new bool[8];
        for (int i = 0; i < 8; i++)
        {
            flags[i] = false;
        }
        flags[7] = includesTime;
        // Pack the bools, using the first bool as the least significant bit
        for (int i = 0; i < 8; i++)
        {
            if (flags[i])
            {
                flagsByte |= (byte)(1 << i);
            }
        }
        return [flagsByte];
    }

    private static bool[] ReadFlagsByte(byte flagsByte)
    {
        bool[] flags = new bool[8];
        for (int i = 0; i < 8; i++)
        {
            flags[i] = (flagsByte & (1 << i)) != 0;
        }
        return flags;
    }

    private static void Flatten(Array varData, int[] varDims, float[] data1D)
    {
        if (varData.LongLength > Int32.MaxValue)
        {
            throw new NotImplementedException("Routines not yet ready for arrays with >2 billion elements");
        }
        int nVals = 1;
        for (int i = 0; i < varDims.Length; i++)
        {
            nVals *= varDims[i];
        }
        Buffer.BlockCopy(varData, 0, data1D, 0, nVals*4);
    }

    public static long[] ReadFileTimes(DataSet ds)
    {
        DateTime refTime = new DateTime(1970, 1, 1, 0, 0, 0);
        int[] timeInts = ds.GetData<int[]>("time");
        string timeUnits = ds.GetAttr<string>("time","units");
        int nTimes = timeInts.Length;
        DateTime[] timeVec = ParseFileTimes(timeUnits,timeInts);
        long[] longTimes = new long[nTimes];
        for (int i = 0; i < nTimes; i++)
        {
            longTimes[i] = (long)(timeVec[i] - refTime).TotalSeconds;
        }
        return longTimes;
    }
    
    private static DateTime[] ParseFileTimes(string units, int[] timeDeltas)
    {
        // Reads a units string (e.g. "minutes since 2023-01-01 00:00:00.0")
        // and a series of integers, returns the corresponding vector of DateTimes
        int nTimes = timeDeltas.Length;
        int secondsMult;
        string[] substrings = units.Split(' ');
        string timeType = substrings[0].ToLower();
        switch (timeType)
        {
            case "seconds":
                secondsMult = 1;
                break;
            case "minutes":
                secondsMult = 60;
                break;
            case "hours":
                secondsMult = 3600;
                break;
            case "days":
                secondsMult = 3600 * 24;
                break;
            default:
                throw new ArgumentException($"Invalid time units {timeType} in string {units}");
        }
        string ymd = substrings[2];
        string hms = substrings[3];
        string ymdhms = $"{ymd} {hms}";
        DateTime refTime = DateTime.Parse(ymdhms);
        DateTime[] timeVec = new DateTime[nTimes];
        for (int i=0; i<nTimes; i++)
        {
            timeVec[i] = refTime.AddSeconds(secondsMult * timeDeltas[i]);
        }
        return timeVec;
    }

    public static int GetRank(string fileName)
    {
        int[] varDims = new int[4];
        using (var stream = File.Open(fileName, FileMode.Open))
        {
            using (var reader = new BinaryReader(stream, Encoding.Unicode))
            {
                // Only read the header
                byte[] memBytes = new byte[(12*2) + (4*4)];
                reader.Read(memBytes);
                Buffer.BlockCopy(memBytes,2*12,varDims,0,4*4);
            }
        }
        int rank = 0;
        for (int i = 0; i < 4; i++)
        {
            if (varDims[i] > 1)
            {
                rank++;
            }
        }
        return rank;
    }

    public static (DateTime[], float[,,]) ReadFile2D(string fileName)
    {
        int[] varDims = new int[4];
        long[] longTimes;
        float[,,] dataArray;
        int nTimes;
        using (var stream = File.Open(fileName, FileMode.Open))
        {
            using (var reader = new BinaryReader(stream, Encoding.Unicode))
            {
                // Only read the header
                byte[] memBytes = new byte[1 + (12*2) + (4*4)];
                reader.Read(memBytes);
                // Flag byte
                bool[] flags = ReadFlagsByte(memBytes[0]);
                bool timesPresent = flags[7];
                // Skip the variable name - need to get the dimensions to know how far to read
                Buffer.BlockCopy(memBytes,1 + 2*12,varDims,0,4*4);
                int nValues = 1;
                for (int i = 0; i < 4; i++)
                {
                    nValues *= varDims[i];
                }

                nTimes = timesPresent ? varDims[0] : 0;
                longTimes = new long[nTimes];
                memBytes = new byte[(nTimes * 8) + (nValues * 4)];
                // Read in all remaining data
                reader.Read(memBytes);
                dataArray = new float[varDims[0],varDims[2],varDims[3]];
                if (timesPresent)
                {
                    Buffer.BlockCopy(memBytes, 0, longTimes, 0, 8 * nTimes);
                }
                Buffer.BlockCopy(memBytes,nTimes*8,dataArray,0,4*nValues);
            }
        }

        // Convert from longTimes to timeVec
        DateTime[] timeVec = new DateTime[nTimes];
        DateTime refTime = new DateTime(1970, 1, 1, 0, 0, 0);
        for (int i = 0; i < nTimes; i++)
        {
            timeVec[i] = refTime + TimeSpan.FromSeconds((double)longTimes[i]);
        }
        return (timeVec, dataArray);
    }
    
    public static (DateTime[], float[,,,]) ReadFile3D(string fileName)
    {
        int[] varDims = new int[4];
        long[] longTimes;
        float[,,,] dataArray;
        int nTimes;
        using (var stream = File.Open(fileName, FileMode.Open))
        {
            using (var reader = new BinaryReader(stream, Encoding.Unicode))
            {
                // Only read the header
                byte[] memBytes = new byte[1 + (12*2) + (4*4)];
                reader.Read(memBytes);
                // Flag byte
                bool[] flags = ReadFlagsByte(memBytes[0]);
                bool timesPresent = flags[7];
                // Skip the variable name - need to get the dimensions to know how far to read
                Buffer.BlockCopy(memBytes,1 + 2*12,varDims,0,4*4);
                int nValues = 1;
                for (int i = 0; i < 4; i++)
                {
                    nValues *= varDims[i];
                }

                nTimes = timesPresent ? varDims[0] : 0;
                longTimes = new long[nTimes];
                memBytes = new byte[(nTimes * 8) + (nValues * 4)];
                // Read in all remaining data
                reader.Read(memBytes);
                dataArray = new float[varDims[0],varDims[1],varDims[2],varDims[3]];
                if (timesPresent)
                {
                    Buffer.BlockCopy(memBytes, 0, longTimes, 0, 8 * nTimes);
                }
                Buffer.BlockCopy(memBytes,nTimes*8,dataArray,0,4*nValues);
            }
        }

        // Convert from longTimes to timeVec
        DateTime[] timeVec = new DateTime[nTimes];
        DateTime refTime = new DateTime(1970, 1, 1, 0, 0, 0);
        for (int i = 0; i < nTimes; i++)
        {
            timeVec[i] = refTime + TimeSpan.FromSeconds((double)longTimes[i]);
        }
        return (timeVec, dataArray);
    }
}
