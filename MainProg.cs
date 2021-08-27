using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace IrrelevantFileRemoval
{
    class MainProg
    {
        private string directory;

        private readonly string blobUrl = "https://thebloburl.blob.core.windows.net/container?sp=theSAStokenandKey%3D";

        private readonly string blobNamesFile = "BlobNames.txt";

        private readonly string moodleDbFile = @"./MoodleFileTable.csv";

        private readonly string finalRemoveList = "FinalList.txt";

        private BlobContainerClient containerClient;

        public MainProg()
        {
            string path = System.Reflection.Assembly.GetExecutingAssembly().Location;
            this.directory = System.IO.Path.GetDirectoryName(path);
            Uri blobUri = new Uri(this.blobUrl);
            this.containerClient = new BlobContainerClient(blobUri);
        }

        public async Task Start(string[] args)
        {
            // Read database values
            IEnumerable<string> dbValues = this.ReadDatabaseValues().Distinct().ToList();
            var blobValueMapping = this.ReadBlobValues();
            IEnumerable<string> blobValues = blobValueMapping.Keys.Select(blobName => blobName.Substring(6)).ToList();
            IEnumerable<string> intersectValues = dbValues.Intersect(blobValues).ToList();

            ulong totalSizeToRemove = 0;
            IEnumerable<string> exceptValues = blobValues
                .Except(intersectValues)
                .Distinct()
                .Select(exceptValue =>
                {
                    string start = exceptValue.Substring(0, 2);
                    string second = exceptValue.Substring(2, 2);
                    string finalValue = start + "/" + second + "/" + exceptValue;
                    if (blobValueMapping.ContainsKey(finalValue))
                    {
                        totalSizeToRemove += blobValueMapping[finalValue];
                    }
                    else
                    {
                        // Console.WriteLine("Could not find size for: " + finalValue);
                    }

                    return finalValue;
                }
                )
                .ToList();

            System.IO.File.WriteAllLines(this.GetFilePath(this.finalRemoveList), exceptValues);

            IEnumerable<string> nonDbValues = dbValues.Except(intersectValues).ToList();
            foreach (var nonValue in nonDbValues)
            {
                Console.WriteLine(nonValue);
            }

            Console.WriteLine("Count in DB = " + dbValues.Count());
            Console.WriteLine("Count in Blob Storage = " + blobValues.Count());
            Console.WriteLine("Intersecting values = " + intersectValues.Count());
            Console.WriteLine("Count to remove = " + exceptValues.Count());
            Console.WriteLine($"Size to remove = {totalSizeToRemove}, {totalSizeToRemove / (1024 * 1024 * 1024)} GB");

            if (args != null)
            {
                Console.WriteLine("Delete begining");
                await this.DeleteBlobs(exceptValues);
            }
        }

        /// <summary>
        /// Enumerates storage container and stores values in a txt
        /// </summary>
        /// <param name="containerClient">Container Client</param>
        /// <returns>Task object</returns>
        private async Task StoreEnumeratedValues()
        {
            var resultSegment = containerClient.GetBlobsAsync().AsPages(default);
            string fileName = this.GetFilePath(this.blobNamesFile);
            await foreach (var blobPage in resultSegment)
            {
                IEnumerable<string> blobNames = blobPage.Values.Select(value => value.Name + ">>>" + value.Properties.ContentLength ?? "NOT-FOUND");
                System.IO.File.AppendAllLines(fileName, blobNames);
            }
        }

        private async Task DeleteBlobs(IEnumerable<string> blobsToDelete)
        {
            foreach(var blobName in blobsToDelete)
            {
                await this.containerClient.DeleteBlobIfExistsAsync(blobName);
                Console.WriteLine($"Deleted {blobName}");
            }
        }

        /// <summary>
        /// Read database values from local file
        /// </summary>
        /// <returns></returns>
        private IEnumerable<string> ReadDatabaseValues()
        {
            // Read all Hashes
            List<string> hashList = new List<string>();
            ulong totalSize = 0;
            using (var reader = new StreamReader(this.moodleDbFile))
            {
                // First line is a bunch of headers
                reader.ReadLine();
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    hashList.Add(values[1].Replace("\"", ""));
                    try
                    {
                        totalSize += Convert.ToUInt64(values[10]);
                    }
                    catch
                    {
                        // Console.WriteLine("Not a valid file size: " + values[10]);
                    }
                }
            }

            Console.WriteLine($"Total Size Database = {totalSize}, {totalSize / (1024 * 1024 * 1024)} GB");
            return hashList;
        }

        private Dictionary<string, ulong> ReadBlobValues()
        {
            // Read all Hashes
            string pathToBlobs = this.GetFilePath(this.blobNamesFile);
            if (!File.Exists(pathToBlobs))
            {
                this.StoreEnumeratedValues().GetAwaiter().GetResult();
            }

            Dictionary<string, ulong> hashList = new Dictionary<string, ulong>();
            using (var reader = new StreamReader(pathToBlobs))
            {
                // First line is a bunch of headers
                reader.ReadLine();

                ulong totalSize = 0;
                while (!reader.EndOfStream)
                {
                    var singleLine = reader.ReadLine();
                    var line = singleLine.Split(">>>");
                    string name = line[0];
                    ulong size = Convert.ToUInt64(line[1]);
                    
                    if (hashList.TryGetValue(name, out _))
                    {
                        continue;
                    }

                    totalSize += size;
                    hashList.Add(name, size);
                }

                Console.WriteLine($"Total size Blob: {totalSize}, {totalSize / (1024 * 1024 * 1024)} GB");
            }

            return hashList;
        }

        /// <summary>
        /// Gets Absolute file path
        /// </summary>
        /// <param name="fileName">File Name</param>
        /// <returns>Absolute File Path</returns>
        private string GetFilePath(string fileName)
        {
            return Path.Combine(this.directory, fileName);
        }
    }
}
