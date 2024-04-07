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
    public static void SerializeVariable(string varName, DataSet ds, string outFile, long[]? fileTimes = null)
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
        if ((longLength * 4) > int.MaxValue - 500)
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
        else if ((targVar.Rank == 1) && (fileTimes == null))
        {
            float[] varData = ds.GetData<float[]>(varName);
            varDims = [1, 1, 1, varData.Length];
            Flatten(varData, varDims, data1D);
        }
        else
        {
            throw new ArgumentException($"Code not in place for a variable of rank {targVar.Rank}");
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
        string varShortName = PrepareVariableName(varName, nameLength);
        using (var stream = File.Open(outFile, FileMode.Create))
        {
            using (var writer = new BinaryWriter(stream, Encoding.Unicode))
            {
                byte[] bigBlock = new byte[flagBytes + nameBytes + dimBytes + timeBytes + (4 * nVals)];
                Buffer.BlockCopy(flags, 0, bigBlock, 0, flagBytes);
                Buffer.BlockCopy(Encoding.Unicode.GetBytes(varShortName), 0, bigBlock, flagBytes, nameBytes);
                Buffer.BlockCopy(varDims, 0, bigBlock, flagBytes + nameBytes, dimBytes);
                if (timeBytes > 0)
                {
                    Buffer.BlockCopy(fileTimes, 0, bigBlock, flagBytes + nameBytes + dimBytes, timeBytes);
                }
                Buffer.BlockCopy(data1D, 0, bigBlock, flagBytes + nameBytes + dimBytes + timeBytes, nVals * 4);
                writer.Write(bigBlock);
            }
        }
    }

    public static string PrepareVariableName(string varName, int nameLength=12)
    {
        return varName.Length > nameLength ? varName.Substring(0, nameLength) : varName.PadRight(nameLength);
    }

    public static void SerializeTime(string outFile, long[] longTimes)
    {
        // Time file is very simple - just the number of times, followed by a vector of them as long integers
        // (seconds since 1970-01-01 00:00:00)
        int nTimes = longTimes.Length;
        int timeBytes = 8 * nTimes;
        using (var stream = File.Open(outFile, FileMode.Create))
        {
            using (var writer = new BinaryWriter(stream, Encoding.Unicode))
            {
                byte[] bigBlock = new byte[4 + timeBytes];
                int[] nTimesArr = [nTimes];
                Buffer.BlockCopy(nTimesArr, 0, bigBlock, 0, 4);
                Buffer.BlockCopy(longTimes, 0, bigBlock, 4, timeBytes);
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
                byte[] memBytes = new byte[1 + (12*2) + (4*4)];
                reader.Read(memBytes);
                Buffer.BlockCopy(memBytes,1 + 2*12,varDims,0,4*4);
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

    public static (DateTime[], float[,,]) Deserialize2D(string fileName)
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
        return (LongTimesToDateTimes(longTimes), dataArray);
    }

    private static DateTime[] LongTimesToDateTimes(long[] longTimes)
    {
        int nTimes = longTimes.Length;
        DateTime[] timeVec = new DateTime[nTimes];
        DateTime refTime = new DateTime(1970, 1, 1, 0, 0, 0);
        for (int i = 0; i < nTimes; i++)
        {
            timeVec[i] = refTime + TimeSpan.FromSeconds((double)longTimes[i]);
        }

        return timeVec;
    }

    public static (DateTime[], float[,,,]) Deserialize3D(string fileName)
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
        return (LongTimesToDateTimes(longTimes), dataArray);
    }

    public static DateTime[] DeserializeTime(string fileName)
    {
        long[] longTimes;
        using (var stream = File.Open(fileName, FileMode.Open))
        {
            using (var reader = new BinaryReader(stream, Encoding.Unicode))
            {
                // Read in number of times (int)
                int nTimes;
                nTimes = reader.ReadInt32();
                longTimes = new long[nTimes];
                // Only read the header
                byte[] memBytes = new byte[8*nTimes];
                //reader.Read(memBytes); // Why was this duplicated?
                reader.Read(memBytes);
                Buffer.BlockCopy(memBytes, 0, longTimes, 0, 8 * nTimes);
            }
        }
        return LongTimesToDateTimes(longTimes);
    }

    private static readonly string[] LevelNameOptions = ["lev", "level", "levs", "levels"];

    public static void SerializeDimensions(DataSet ds, string fileName, long[]? longTimes)
    {
        float[] lons = ds.GetData<float[]>("lon");
        float[] lats = ds.GetData<float[]>("lat");
        int nLevels =
            (from levName in LevelNameOptions
                where ds.Variables.Contains(levName)
                select ds.GetData<float[]>(levName).Length).FirstOrDefault();
        int[]? levVec = nLevels > 0 ? new[] { nLevels } : null;
        
        using (var stream = File.Open(fileName, FileMode.Create))
        {
            using (var writer = new BinaryWriter(stream, Encoding.Unicode))
            {
                Write1DVector(writer,longTimes,8,"TIME");
                Write1DVector(writer,levVec,4,"LEVELS");
                Write1DVector(writer,lats,4,"LAT1D");
                Write1DVector(writer,lons,4,"LON1D");
            }
        }
    }

    private static void Write1DVector(BinaryWriter writer, Array? vec, int varSize, string varName)
    {
        int vecLen = vec?.Length ?? 0;
        // Record variable name, number of values, and then the variable itself
        byte[] memBytes = new byte[12 * 2 + 1 * 4 + vecLen * varSize];
        Buffer.BlockCopy(Encoding.Unicode.GetBytes(PrepareVariableName(varName)), 0, memBytes, 0, 12 * 2);
        int[] intVec = [vecLen];
        Buffer.BlockCopy(intVec, 0, memBytes, 12 * 2, 4);
        if (vec != null)
        {
            Buffer.BlockCopy(vec, 0, memBytes, 12 * 2 + 4, vecLen * varSize);
        }
        writer.Write(memBytes);
    }
    public static (DateTime[]?, int?, float[]?, float[]?, bool[]) DeserializeDimensions(string fileName)
    {
        float[]? lons = null;
        float[]? lats = null;
        int? nLevels = null;
        DateTime[]? timeVec = null;
        bool[] dimsFound = new bool[4];
        for (int i = 0; i < 4; i++)
        {
            dimsFound[i] = false;
        }
        using (var stream = File.Open(fileName, FileMode.Open))
        {
            using (var reader = new BinaryReader(stream, Encoding.Unicode))
            {
                // Read in variable name and number of entries
                byte[] memBytes;
                char[] varName = new char[12];
                int[] intVec = new int[1];
                // Loop until we reach end of file
                while (true)
                {
                    memBytes = new byte[(12 * 2) + 4];
                    // Read in the data - or break if at EOF
                    if (reader.Read(memBytes) == 0) { break; }
                    Buffer.BlockCopy(memBytes, 0, varName, 0, 12 * 2);
                    Buffer.BlockCopy(memBytes, 12 * 2, intVec, 0, 4);
                    int nEntries = intVec[0];
                    string varString = new string(varName).TrimEnd();
                    //Console.WriteLine($"{varString} has {nEntries} entries");
                    switch (varString)
                    {
                        case "TIME":
                            dimsFound[0] = true;
                            if (nEntries == 0) { break; }
                            long[] longTimes = new long[nEntries];
                            memBytes = new byte[nEntries * 8];
                            reader.Read(memBytes);
                            Buffer.BlockCopy(memBytes,0,longTimes,0,nEntries*8);
                            timeVec = LongTimesToDateTimes(longTimes);
                            break;
                        case "LEVELS":
                            dimsFound[1] = true;
                            if (nEntries == 0) { break; }
                            memBytes = new byte[4];
                            reader.Read(memBytes);
                            Buffer.BlockCopy(memBytes, 0, intVec, 0, 4);
                            nLevels = intVec[0];
                            break;
                        case "LAT1D":
                            dimsFound[2] = true;
                            if (nEntries == 0) { break; }
                            memBytes = new byte[nEntries * 4];
                            reader.Read(memBytes);
                            lats = new float[nEntries];
                            Buffer.BlockCopy(memBytes, 0, lats, 0, 4 * nEntries);
                            break;
                        case "LON1D":
                            dimsFound[3] = true;
                            if (nEntries == 0) { break; }
                            memBytes = new byte[nEntries * 4];
                            reader.Read(memBytes);
                            lons = new float[nEntries];
                            Buffer.BlockCopy(memBytes, 0, lons, 0, 4 * nEntries);
                            break;
                        default:
                            throw new InvalidOperationException(
                                $"Dimension {varString} not recognized during deserialization.");
                    }
                }
            }
        }
        return (timeVec, nLevels, lats, lons, dimsFound);
    }

    public static float[] Deserialize1DNoTime(string fileName)
    {
        (_, float[,] data2D) = Deserialize1D(fileName);
        float[] data1D = new float[data2D.Length];
        Buffer.BlockCopy(data2D,0,data1D,0,data2D.Length * 4);
        return data1D;
    }
    public static (DateTime[], float[,]) Deserialize1D(string fileName)
    {
        int[] varDims = new int[4];
        long[] longTimes;
        float[,] dataArray;
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
                dataArray = new float[varDims[0],varDims[3]];
                if (timesPresent)
                {
                    Buffer.BlockCopy(memBytes, 0, longTimes, 0, 8 * nTimes);
                }
                Buffer.BlockCopy(memBytes,nTimes*8,dataArray,0,4*nValues);
            }
        }
        return (LongTimesToDateTimes(longTimes), dataArray);
    }
}
