using System;
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TPP.Concurrency.PLINQ {

    class PLinq {

        static double VectorModulusLinq(IEnumerable<short> vector) {
            return Math.Sqrt(vector.Select(vi => (long)vi * vi).Aggregate((vi, vj) => vi + vj));
            // return Math.Sqrt(v.Aggregate((acc, e) => acc += e * e));
        }

        static double VectorModulusPlinq(IEnumerable<short> vector) {
            return Math.Sqrt(vector.AsParallel().Select(i => (long)i * i).Aggregate((res, x) => res + x));
            // return Math.Sqrt(vector.AsParallel().Aggregate(0L, (acc, item) => acc + item * item));
        }

        static double VectorModulusParallelInterlocked(short[] vector)
        {
            int result = 0;

            Parallel.ForEach(vector, (x) =>
            {
                int square = x * x;
                Interlocked.Add(ref result, square);
            });

            return Math.Sqrt(result);
        }

        static double HighComputationLinq(IEnumerable<short> vector) {
            return Math.Sqrt(
                vector.Select(vi => vi == 0 ? 1.0 : Math.PI / Math.Pow(Math.E, Math.Abs(vi)))
                    .Aggregate((vi, vj) => Math.Sqrt(Math.Abs(vi))/Math.Sqrt(Math.Abs(vi))));
        }

        static double HighComputationPlinq(IEnumerable<short> vector) {
            return Math.Sqrt(
                vector.AsParallel()
                .Select(vi => vi == 0 ? 1.0 : Math.PI / Math.Pow(Math.E, Math.Abs(vi)))
                    .Aggregate((vi, vj) => Math.Sqrt(Math.Abs(vi)) / Math.Sqrt(Math.Abs(vi))));
        }

        static short[] CreateRandomVector(int numberOfElements, short lowest, short greatest)
        {
            short[] vector = new short[numberOfElements];
            Random random = new Random();
            for (int i = 0; i < numberOfElements; i++)
                vector[i] = (short)random.Next(lowest, greatest + 1);
            return vector;
        }

        static int[] CreateRandomIntVector(int numberOfElements, short lowest, short greatest)
        {
            int[] vector = new int[numberOfElements];
            Random random = new Random();
            for (int i = 0; i < numberOfElements; i++)
                vector[i] = (short)random.Next(lowest, greatest + 1);
            return vector;
        }

        static IDictionary<short, int> CountSequential(short[] v)
        {
            IDictionary<short, int> dic = new Dictionary<short, int>();

            foreach(var element in v)
            {
                lock (dic)
                {
                    if (dic.ContainsKey(element))
                        dic[element]++;
                    else
                        dic.Add(element, 1);
                }
            }

            return dic;            
        }

        static IDictionary<short, int> CountParallelFor(short[] v)
        {
            IDictionary<short, int> dic = new Dictionary<short, int>();

            Parallel.ForEach(v, (element) =>
            {
                lock (dic)
                {
                    if (dic.ContainsKey(element))
                        dic[element]++;
                    else
                        dic.Add(element, 1);
                }
            });

            return dic;

            /*
             //PARALLEL.FOR APPROACH
             Parallel.For(0, v.Length, (i) =>
             {
                 lock (dic)
                 {
                     if (dic.ContainsKey(v[i]))
                         dic[v[i]]++;
                     else
                         dic.Add(v[i], 1);
                 }                
             });
            */
        }

        //EX1.2 Split data in odd and even positions and aggregate both at the end
        static IDictionary<short, int> CountInvoke(short[] v)
        {
            IDictionary<short, int> dic1 = new Dictionary<short, int>();
            IDictionary<short, int> dic2 = new Dictionary<short, int>();

            Parallel.Invoke(
                () => {
                    for (int i = 0; i < v.Length; i += 2)
                    {
                        lock (dic1)
                        {
                            if (dic1.ContainsKey(v[i]))
                                dic1[v[i]]++;
                            else
                                dic1.Add(v[i], 1);
                        }                        
                    }
                },
                () => {
                    for (int i = 1; i < v.Length; i += 2)
                    {
                        lock (dic2)
                        {
                            if (dic2.ContainsKey(v[i]))
                                dic2[v[i]]++;
                            else
                                dic2.Add(v[i], 1);
                        }
                    }
                }
                );

            foreach (var item in dic2)
            {
                if (dic1.ContainsKey(item.Key))
                    dic1[item.Key] += dic2[item.Key];
                else
                    dic1.Add(item.Key, item.Value);
            }

            return dic1;
        }

        // A is aggregated in B
        static IDictionary<short, int> DictAggregate(IDictionary<short, int> a, IDictionary<short, int> b)
        {
            foreach (var item in a)
            {
                if (b.ContainsKey(item.Key))
                    b[item.Key] += item.Value;
                else
                    b.Add(item);
            }

            return b;
        }

        //EX1.3 
        static IDictionary<short, int> CountInvokeN(short[] v)
        {
            IDictionary<short, int> dic1 = new Dictionary<short, int>();
            IDictionary<short, int> dic2 = new Dictionary<short, int>();

            int n = 1;
            Parallel.Invoke(
                () => {
                    for (int i = 0; i < v.Length; i = n*i)
                    {
                        if (dic1.ContainsKey(v[i]))
                            dic1[v[i]]++;
                        else
                            dic1.Add(v[i], 1);
                        n++;
                    }
                },
                () => {
                    for (int i = 1; i < v.Length; i = n* i + 1)
                    {
                        if (dic2.ContainsKey(v[i]))
                            dic2[v[i]]++;
                        else
                            dic2.Add(v[i], 1);
                        n++;
                    }
                }
                );

            foreach (var e in dic2)
            {
                if (dic1.ContainsKey(e.Key))
                    dic1[e.Key] += dic2[e.Key];
                else
                    dic1.Add(e.Key, e.Value);
            }

            return dic1;
        }

        static void ShowOrderKey<T>(IDictionary<short, int> d)
        {
            foreach (var e in d.OrderBy(x => x.Key))
                Console.Write(e);
        }

        static double VarLinq(IEnumerable<short> vector)
        {
            var mean = vector.AsParallel().Aggregate(0.0, (acc, el) => acc + el) / vector.Count();
            return vector.Aggregate(0.0, (acc, el) => acc + Math.Pow((el - mean), 2.0)) / vector.Count();
        }

        static double Mean(IEnumerable<short> vector)
        {
            double result = 0;

            Parallel.Invoke(() => result = vector.Aggregate(0.0, (acc, el) => acc + el) / vector.Count());

            return result;
        }

        static double MeanAsparallel(IEnumerable<short> vector)
        {
            double result = 0;

            Parallel.Invoke(() => result = vector.AsParallel().Aggregate(0.0, (acc, el) => acc + el) / vector.Count());

            return result;
        }      
     
        static int SumDict(IDictionary<short, int> dic)
        {
            return dic.Aggregate(0, (acc, p) => acc + p.Value);
        }

        static int SumDict2(int[] d, int[] d2)
        {
            int result = 0;

            foreach (var e in d)
                result += e;
            foreach (var e in d2)
                result += e;
            return result;
        }

        static bool IsPrime(int n)
        {
            if (n < 0)
                return false;
            else if (n < 2)
                return true;

            for (int i = 2; i < n; i++)
                if (n % i == 0)
                    return false;

            return true;
        }

        static int SerialLastOccurenceIndex(int[] v, int val)
        {
            int result = -1;
            for (int i = 0; i < v.Length; i++)
                if (v[i] == val)
                    result = i;
            return result;
        }

        static int ParallelLastOccurenceIndex(int[] v, int val)
        {
            int result = -1;
            Object myLock = new Object();

            Parallel.For(0, v.Length, (i) =>
            {
                if (v[i] == val)
                {
                    lock (myLock)
                    {
                        if (result < 0)
                            result = i;
                        else if (result < i)
                            result = i;
                    }
                }                    
            });

            return result;
        }

        static IDictionary<short, int> countForSeq(short[] v)
        {
            IDictionary<short, int> result = new Dictionary<short, int>();
            for (int i = 0; i < v.Length; i++)
            {
                if (result.ContainsKey(v[i]))
                    result[v[i]]++;
                else
                    result.Add(v[i], 1);
            }
            return result;
        }

        static void Show<T>(IEnumerable<T> list, char sep = ' ', char end = '\n')
        {
            foreach (var e in list)
                Console.Write("{0}{1}", e, sep);
            Console.Write(end);
        }

        static void Main()
        {
            short[] vector = CreateRandomVector(50, -10, 10);
            Show(vector);

            DateTime before = DateTime.Now;
            var modulusLINQ = VectorModulusLinq(vector);
            DateTime after = DateTime.Now;
            long millis = (after - before).Ticks;
            Console.WriteLine("\nVector modulus LINQ: {0}  milliseconds {1}", modulusLINQ, millis);

            before = DateTime.Now;
            var modulusPLINQ = VectorModulusPlinq(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Console.WriteLine("Vector modulus PLINQ: {0}  milliseconds {1}", modulusPLINQ, millis);

            before = DateTime.Now;
            var moodulusForeach = VectorModulusParallelInterlocked(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Console.WriteLine("Vector modulus Interlocked: {0}  milliseconds {1}", modulusLINQ, millis);


            before = DateTime.Now;
            var highCompuLINQ = HighComputationLinq(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Console.WriteLine("\nHigh computation LINQ: {0}  milliseconds {1}", highCompuLINQ, millis);

            before = DateTime.Now;
            var highCompupLINQ = HighComputationPlinq(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Console.WriteLine("High computation PLINQ: {0}  milliseconds {1}\n", highCompupLINQ, millis);


            before = DateTime.Now;
            var result = CountSequential(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Show(result);
            Console.WriteLine("Sum Dic {0}", SumDict(result));
            Console.WriteLine("Elapsed time in sequential {0}\n\n", millis);

            before = DateTime.Now; 
            result = CountParallelFor(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Show(result);
            Console.WriteLine("Sum Dic {0}", SumDict(result));
            Console.WriteLine("Elapsed time in PARALLEL FOREACH {0}\n\n", millis);

            before = DateTime.Now;
            result = CountInvoke(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Show(result);
            Console.WriteLine("Sum Dic {0}", SumDict(result));
            Console.WriteLine("Elapsed time in PARALLEL INVOKE {0}\n\n", millis);

            before = DateTime.Now;
            result = CountInvokeN(vector);
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Show(result);
            Console.WriteLine("Sum Dic {0}", SumDict(result));
            Console.WriteLine("Elapsed time in PARALLEL INVOKE EVEN / ODD {0}\n\n", millis);


            before = DateTime.Now;
            Console.WriteLine(Mean(vector));
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Console.WriteLine("Elapsed time in Mean {0}\n\n", millis);

            before = DateTime.Now;
            Console.WriteLine(MeanAsparallel(vector));
            after = DateTime.Now;
            millis = (after - before).Ticks;
            Console.WriteLine("Elapsed time in Mean As Parallel {0}\n\n", millis);


            short[] primesFor = new short[vector.Length];
            short[] primesForEach = new short[vector.Length];

            int j = 0;
            Parallel.For(0, vector.Length, (i) =>
            {
                if (IsPrime(vector[i]))
                {
                    primesFor[j] = vector[i];
                    j++;
                }
            });

            Console.WriteLine("PRIMES FOR");
            Show(primesFor.Distinct().OrderBy(x => x));

            j = 0;
            Parallel.ForEach(vector, (element) =>
            {
                if (IsPrime(element))
                {
                    primesForEach[j] = element;
                    j++;
                }
            });

            Console.WriteLine("\nPRIMES FOREACH");
            Show(primesForEach.Distinct().OrderBy(x => x));

            var primesPlinq = vector.AsParallel().Where(x => IsPrime(x));
            Console.WriteLine("\nPRIMES PLINQ");
            Show(primesPlinq.Distinct().OrderBy(x => x));


            Console.WriteLine("\nZIP");

            var zip = primesFor.Distinct().OrderBy(x => x).Zip(primesForEach.Distinct());

            foreach (var e in zip)
                Console.WriteLine(e);

            // To make sure the return the same prime numbers ordered in ascending
            Console.WriteLine("\nZIP ORDERED");

            var zipOrdered = primesFor.Distinct().OrderBy(x => x).Zip(primesForEach.Distinct().OrderBy(x => x));

            foreach (var e in zipOrdered)
            {
                Debug.Assert(e.First.Equals(e.Second));
                Console.WriteLine(e);
            }

            int[] a = CreateRandomIntVector(100, -10, 10),
                b = CreateRandomIntVector(100, -10, 10);
                //c = SumDict(a, b);
        }
    }
}