using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace IrrelevantFileRemoval
{
    class Program
    {
        static async Task Main(string[] args)
        {
            MainProg mainProg = new MainProg();
            Console.WriteLine("Enter 1 for listing and 2 for deleting operation");
            int option = Convert.ToInt32(Console.ReadLine());
            string[] x;
            if (option == 1)
            {
                x = null;
            }
            else
            {
                x = new string[1];
            }

            await mainProg.Start(x);
        }
    }
}
