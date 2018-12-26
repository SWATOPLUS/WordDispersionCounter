using System;
using System.IO;
using System.Linq;
using System.Reflection;
using MoreLinq;

namespace LineMixer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("usage <file> <parts>");
            }

            var file = args[0];
            var parts = int.Parse(args[1]);

            var random = new Random();

            MixFile(file, random);

            var lines = File.ReadLines(file).Count();
            var batchSize = lines / parts; 

            if(lines % parts != 0)
            {
                batchSize++;
            }

            var i = 0;

            foreach (var batch in File.ReadLines(file).Batch(batchSize))
            {
                File.AppendAllLines(GetVolumeName(file, i), batch);
                i++;
            }
        }

        private static void MixFile(string file, Random random)
        {
            var mixedLines = File.ReadLines(file).Shuffle(random).ToArray();

            File.WriteAllLines(file, mixedLines);
        }

        private static string GetVolumeName(string file, int volume)
        {
            var name = Path.GetFileNameWithoutExtension(file);
            var ext = Path.GetExtension(file);

            return $"{name}-{volume}{ext}";
        }
    }
}
