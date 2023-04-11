using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using CsvHelper;
using Dynamitey;
using CsvDataReader = Sylvan.Data.Csv.CsvDataReader;

namespace MultiLabelCSVTransformer
{
    internal class Program
    {
        static unsafe void Main(string[] args)
        {
            const string Ext = ".csv";
            
            const char Separator = ',';
            
            var ExtSpan = Ext.AsSpan();
            
            GetPath:
            Console.WriteLine("Input path to CSV! ( Or just drag it in ;) )");

            var Path = Console.ReadLine();
            
            var PathSpan = Path.AsSpan();

            // //0, 1, 2, 3 | Length: 4
            // if (PathSpan.Length < ExtSpan.Length || !PathSpan.Slice(PathSpan.Length - ExtSpan.Length).SequenceEqual(ExtSpan))
            // {
            //     Console.WriteLine($"Invalid {Ext} file!");
            //     
            //     goto GetPath;
            // }

            string ErrorMsg;
            
            try
            {
                using var RStream = new StreamReader(Path);

                using var Reader = CsvDataReader.Create(RStream);

                using var WStream = new StreamWriter($"Output{Ext}", append: false);

                using var Writer = new CsvWriter(WStream, CultureInfo.InvariantCulture);

                //var Header = Reader.HeaderRecord;

                var Header = Reader.GetColumnSchema();

                //var SelectedColumnsHS = Header!.ToDictionary(x => x.ColumnName);

                var SelectedColumnsList = Header.ToList();
                
                var ExcludedColumns = new List<DbColumn>();
                
                //We pre-allocate like half the memory ( sizeof(char) is 2 bytes )
                var SB = new StringBuilder((int) new FileInfo(Path).Length);
                
                SB.Append($"Select columns to exclude via 0-based indexes, separated with ({Separator})");
                
                var ColumnIndex = 0;
                
                foreach (var Column in Header)
                {
                    SB.Append($"\n{ColumnIndex++} | {Column.ColumnName}");
                }
                
                var ColumnSelectPrompt = SB.ToString();
                
                ColumnSelectPrompt:
                Console.WriteLine(ColumnSelectPrompt);
                
                var ColumnSelectResponse = Console.ReadLine().AsSpan();
                
                while (true)
                {
                    var IndexOfSeparator = ColumnSelectResponse.IndexOf(Separator);
                
                    var MoveNext = IndexOfSeparator != -1;
                
                    ReadOnlySpan<char> CurrentSegment;
                    
                    if (MoveNext)
                    {
                        CurrentSegment = ColumnSelectResponse.Slice(0, IndexOfSeparator);
                    }
                
                    else
                    {
                        CurrentSegment = ColumnSelectResponse;
                    }
                
                    if (!int.TryParse(CurrentSegment, out var ExcludedRowIndex) || (uint) ExcludedRowIndex >= Header.Count)
                    {
                        goto MalformedIndex;
                    }
                
                    var Excluded = Header[ExcludedRowIndex];
                
                    ExcludedColumns.Add(Excluded);
                    
                    SelectedColumnsList.Remove(Excluded);
                
                    if (MoveNext)
                    {
                        ColumnSelectResponse = ColumnSelectResponse.Slice(IndexOfSeparator + 1);
                        
                        continue;
                    }
                
                    break;
                    
                    MalformedIndex:
                    Console.WriteLine("Malformed index!");
                    goto ColumnSelectPrompt;
                }
                
                var SelectedColumns = SelectedColumnsList.Select(x => x.ColumnOrdinal!.Value).ToArray();
                
                SB.Clear();
                
                SB.Append("The following columns are selected:");
                
                ColumnIndex = 0;
                
                foreach (var Column in SelectedColumns)
                {
                    SB.Append($"\n{ColumnIndex++} | {Column}");
                }
                
                SB.Append($"\nProceed? ( Y / N )");
                
                ProceedPrompt:
                var ProceedPrompt = SB.ToString();
                
                Console.WriteLine(ProceedPrompt);
                
                switch (Console.ReadLine().ToUpper())
                {
                    default: 
                        goto ProceedPrompt;
                    case "Y":
                        break;
                    case "N":
                        goto ColumnSelectPrompt;
                }
                
                SB.Clear();
                
                // foreach (var Column in ExcludedColumnsOrdinal)
                // {
                //     SB.Append(Column);
                //     
                //     SB.Append(Separator);
                // }
                //
                // //Separator already inserted for us
                // SB.Append("Label");
                
                dynamic Record = new ExpandoObject();
                
                foreach (var Column in ExcludedColumns)
                {
                    Dynamic.InvokeSet(Record, Column.ColumnName, string.Empty);
                }

                var LabelBox = (object) 0;

                ref var Label = ref Unsafe.Unbox<int>(LabelBox);
                
                Dynamic.InvokeSet(Record, "Label", LabelBox);
                
                // //Separator already inserted for us
                // SB.Append("Label");

                var IndexOfColumnsWithOne = new List<int>(SelectedColumns.Length);
                
                ref var FirstColumn = ref MemoryMarshal.GetArrayDataReference(SelectedColumns);
                
                ref var LastColumnOffsetByOne = ref Unsafe.Add(ref FirstColumn, SelectedColumns.Length);
                
                var RowWithNoSetColumnCount = 0;
                
                while (Reader.Read())
                {
                    for (ref var Current = ref FirstColumn
                         ; !Unsafe.AreSame(ref Current, ref LastColumnOffsetByOne)
                         ; Current = ref Unsafe.Add(ref Current, 1))
                    {
                        //var Int = Reader.GetField<int>(Current);

                        var Int = Reader.GetInt16(Current);
                        
                        if (Int == 1)
                        {
                            //var IndexOfCurrent = (int) Unsafe.ByteOffset(ref FirstColumn, ref Current) / sizeof(string);
                            var IndexOfCurrent = (int) Unsafe.ByteOffset(ref FirstColumn, ref Current) / sizeof(int);
                            
                            IndexOfColumnsWithOne.Add(IndexOfCurrent);
                        }
                    }

                    using var IndexOfColumnsWithOneEnumerator = IndexOfColumnsWithOne.GetEnumerator();
                
                    var SetColumnsPresent = IndexOfColumnsWithOneEnumerator.MoveNext();
                
                    // //Remember to append a new line every iteration
                    // SB.Append('\n');
                    
                    //var CurrentStart = SB.Length;
                
                    foreach (var Column in ExcludedColumns)
                    {
                        var Data = Reader.GetFieldSpan(Column.ColumnOrdinal.Value);

                        var ColName = Column.ColumnName;
                        
                        if (!int.TryParse(Data, out var DataAsNum))
                        {
                            Dynamic.InvokeSet(Record, ColName, Data.ToString());
                        }
                
                        else
                        {
                            Dynamic.InvokeSet(Record, ColName, DataAsNum);
                        }

                        // var Data = Reader.GetField<string>(ExcludedColumn);
                        //
                        // if (!int.TryParse(Data, out var DataAsNum))
                        // {
                        //     SB.Append('"');
                        //     SB.Append(Data);
                        //     SB.Append('"');
                        // }
                        //
                        // else
                        // {
                        //     SB.Append(DataAsNum);
                        // }
                        //
                        // SB.Append(Separator);
                    }
                
                    // //0, 1, 2 ( Size: 3 ) -> 3 - 0 -> 3
                    // var ExcludedData = SB.ToString(CurrentStart, SB.Length - CurrentStart);
                
                    if (SetColumnsPresent)
                    {
                        do
                        {
                            Label = IndexOfColumnsWithOneEnumerator.Current;
                            Writer.WriteRecord(Record);
                        } while (IndexOfColumnsWithOneEnumerator.MoveNext());
                        
                        //Remember to clear data, as list is reused next iteration
                        IndexOfColumnsWithOne.Clear();
                    }

                    else
                    {
                        Label = 0;
                        RowWithNoSetColumnCount++;
                        Writer.WriteRecord(Record);
                    }

                    // //Separator already inserted for us, so just insert index of column with set ( 1 ) data
                    // SB.Append(IndexOfColumnsWithOneEnumerator.Current);
                    //
                    // while (IndexOfColumnsWithOneEnumerator.MoveNext())
                    // {
                    //     SB.Append('\n');
                    //     
                    //     SB.Append(ExcludedData);
                    //
                    //     SB.Append(IndexOfColumnsWithOneEnumerator.Current);
                    // }

                    //NoSetColumn:
                    //SB.Append(0);
                }
                
                //File.WriteAllText($"Output{Ext}", SB.ToString());

                Console.WriteLine($"Complete! With {RowWithNoSetColumnCount} row(s) with no set labels!");
                
                return;
            }

            catch (Exception Ex)
            {
                ErrorMsg = $"Failed with exception - {Ex}";
                
                goto PrintError;
            }

            Empty:
            ErrorMsg = "File is empty!";
            goto PrintError;
            
            NoHeader:
            ErrorMsg = "No Header!";
            goto PrintError;

            PrintError:
            Console.WriteLine(ErrorMsg);
            goto GetPath;
        }
    }
}