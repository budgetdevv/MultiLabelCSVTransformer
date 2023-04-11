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
    internal static class Program
    {
        private static void Main()
        {
            const string Ext = ".csv";
            
            const char Separator = ',';
            
            GetPath:
            Console.WriteLine("Input path to CSV! ( Or just drag it in ;) )");

            var Path = Console.ReadLine();

            string ErrorMsg;
            
            try
            {
                using var RStream = new StreamReader(Path);

                using var Reader = CsvDataReader.Create(RStream);

                using var WStream = new StreamWriter($"Output{Ext}", append: false);

                using var Writer = new CsvWriter(WStream, CultureInfo.InvariantCulture);

                var Header = Reader.GetColumnSchema();

                var SelectedColumnsList = Header.ToList();
                
                var ExcludedColumns = new List<DbColumn>();

                var SelectPrompt = $"Select columns to exclude via 0-based indexes, separated with ({Separator})";
                
                var SB = new StringBuilder(1024);
                
                SB.Append(SelectPrompt);
                
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
                
                SB.Append("\nProceed? ( Y / N )");
                
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

                //Mainly to ensure that it is not used after ( IDE would warn )
                SB = null;

                dynamic Record = new ExpandoObject();
                
                foreach (var Column in ExcludedColumns)
                {
                    var ColumnName = Column.ColumnName;
                    Writer.WriteField(ColumnName);
                    Dynamic.InvokeSet(Record, ColumnName, string.Empty);
                }

                var LabelBox = (object) 0;

                ref var Label = ref Unsafe.Unbox<int>(LabelBox);

                const string LabelName = "Label";
                
                Writer.WriteField(LabelName);
                
                Dynamic.InvokeSet(Record, LabelName, LabelBox);
                
                Writer.NextRecord();

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
                        var Int = Reader.GetInt16(Current);
                        
                        if (Int == 1)
                        {
                            var IndexOfCurrent = (int) Unsafe.ByteOffset(ref FirstColumn, ref Current) / sizeof(int);
                            
                            IndexOfColumnsWithOne.Add(IndexOfCurrent);
                        }
                    }

                    using var IndexOfColumnsWithOneEnumerator = IndexOfColumnsWithOne.GetEnumerator();
                
                    var SetColumnsPresent = IndexOfColumnsWithOneEnumerator.MoveNext();

                    foreach (var Column in ExcludedColumns)
                    {
                        var Data = Reader.GetFieldSpan(Column.ColumnOrdinal.Value);

                        Dynamic.InvokeSet(Record, Column.ColumnName, Data.ToString());
                    }

                    if (SetColumnsPresent)
                    {
                        do
                        {
                            //Zero represents neutral or no data
                            Label = IndexOfColumnsWithOneEnumerator.Current + 1;
                            Writer.WriteRecord(Record);
                            Writer.NextRecord();
                        } while (IndexOfColumnsWithOneEnumerator.MoveNext());
                        
                        //Remember to clear data, as list is reused next iteration
                        IndexOfColumnsWithOne.Clear();
                    }

                    else
                    {
                        Label = 0;
                        RowWithNoSetColumnCount++;
                        Writer.WriteRecord(Record);
                        Writer.NextRecord();
                    }
                }

                Console.WriteLine($"Complete! With {RowWithNoSetColumnCount} row(s) with no set labels!");
                
                return;
            }

            catch (Exception Ex)
            {
                ErrorMsg = $"Failed with exception - {Ex}";
                
                goto PrintError;
            }
            
            PrintError:
            Console.WriteLine(ErrorMsg);
            goto GetPath;
        }
    }
}