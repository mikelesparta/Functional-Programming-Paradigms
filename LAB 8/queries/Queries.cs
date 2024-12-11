using System;
using System.Collections.Generic;
using System.Linq;

namespace TPP.Laboratory.Functional.Lab08
{

    class Query
    {
        private Model model = new Model();

        static void Main(string[] args)
        {
            Query query = new Query();

            query.Query1();
            query.Query2();
            query.Query3();
            query.Query4();
            query.Query5();
            query.Query6();
            
            query.Homework1();
            query.Homework2();
            query.Homework3();
            query.Homework4();
            query.Homework5();
        }

        // Check out:
        //  http://msdn.microsoft.com/en-us/library/9eekhta0.aspx
        //  https://msdn.microsoft.com/es-es/library/bb397933.aspx


        private void Query1()
        {
            // Show the phone calls that lasted more than 15 seconds

            Console.WriteLine("*** *** *** QUERY 1 *** *** ***\n");

            // Query syntax
            var q1 =
                from pc in model.PhoneCalls
                where pc.Seconds > 15
                select pc;

            foreach (var item in q1)
                Console.WriteLine(item);


            // Method syntax
            Console.WriteLine();

            var q1_m = model.PhoneCalls.Where(pc => pc.Seconds > 15);

            foreach (var item in q1_m)
                Console.WriteLine(item);
        }
		
		private void Query2()
        {
            // Show the name and surname of the employees older than 50 that work in Cantabria

            Console.WriteLine("\n\n*** *** *** QUERY 2 *** *** ***\n");

            var q2 = model.Employees.Where(x => x.Age > 50 && x.Province.Equals("Cantabria"))
                .Select(x => x.Name + " " + x.Surname);

            foreach (var item in q2)
                Console.WriteLine(item);
        }
		
		private void Query3()
        {
            // Show the names of the departments with more than one employee

            Console.WriteLine("\n\n*** *** *** QUERY 3 *** *** ***\n");

            var q3 = model.Departments.Where(d => d.Employees.Count() > 1).Select(d => d.Name);

            foreach (var item in q3)
                Console.WriteLine(item);
        }
        
        private void Query4()
        {
            // Show the phone calls of each employee ordered by employee name. 
            // Each line should show the name of the employee and the phone call duration in seconds.

            Console.WriteLine("\n\n*** *** *** QUERY 4 *** *** ***\n");

            var q4 = model.Employees.Join(model.PhoneCalls, e => e.TelephoneNumber, p => p.SourceNumber,
                (e, p) => new
                {
                    nombre = e.Name,
                    duration = p.Seconds
                }).OrderBy(x => x.nombre).Select(x => x.nombre + ": " + x.duration);

            foreach (var item in q4)
                Console.WriteLine(item);
        }
		
		private void Query5()
        {
            // Show, grouped by province, the name of the employees 
			
            Console.WriteLine("\n\n*** *** *** QUERY 5 *** *** ***\n");
                       
            var p5 = model.Employees.Select(e => new
            {
                nombre = e.Name,
                provincia = e.Province
            }).GroupBy(x => x.provincia);
                        
            var q5 = model.Employees.GroupBy(e => e.Province);
            
            foreach (var item in p5)
            {
                Console.WriteLine("Province: {0} ", item.Key);
                foreach (var element in item)
                    Console.WriteLine("\tName: {0} ", element.nombre);

                Console.WriteLine();
            }


            //number of employyes by province
            
            var q5_2 = model.Employees.GroupBy(e => e.Province)
                .Select(g => new
                {
                    province = g.Key,
                    numberEmployees = g.Count()
                });

            Console.WriteLine("Number of employyes by province");
            foreach (var item in q5_2)
                Console.WriteLine(item);


            //Provincia con el mínimo número de empleados
           
            var q5_3 = model.Employees.GroupBy(e => e.Province)
                .Select(g => new
                {
                    province = g.Key,
                    numberEmployees = g.Count()
                }).Aggregate((res, x) =>
                res.numberEmployees > x.numberEmployees ? x : res);
            
                Console.WriteLine("\nProvince with min number: {0}", q5_3);

            var q5_32 = model.Employees.GroupBy(x => x.Province).Select(x => new
            {
                province = x.Key,
                numberEmployees = x.Count()
            }).OrderBy(y => y.numberEmployees);

            Console.WriteLine("\nProvince with min number: {0}", q5_32.First());
        }
		
		private void Query6()
        {
            // Show the phone calls done by employees in each department (grouped by departement)

            Console.WriteLine("\n\n*** *** *** QUERY 6 *** *** ***\n");

            var q6 = model.PhoneCalls.Join(model.Employees, p => p.SourceNumber, e => e.TelephoneNumber,
                (p, e) => new
                {
                    department = e.Department,
                    caller = e.Name,
                    sourceNum = p.SourceNumber,
                    destNum = p.DestinationNumber
                }).GroupBy(x => x.department);

            foreach (var item in q6)
            {
                Console.WriteLine(item.Key);

                foreach (var e in item)
                    Console.WriteLine("\tFrom " + e.sourceNum + " to " + e.destNum);
            }
        }

        private void Homework1()
        {
            // Show, ordered by age, the names of the employees in the Computer Science department, 
            // who have an office in the Faculty of Science, 
            // and who have done phone calls longer than one minute

            Console.WriteLine("\n\n*** *** *** HOMEWORK 1 *** *** ***\n");

            var q1 = model.Employees.Where(e => e.Department.Name.Equals("Computer Science") 
            && e.Office.Building.Equals("Faculty of Science")).OrderBy(e => e.Age);
                       
            var q1_2 = q1.Join(model.PhoneCalls, e => e.TelephoneNumber, p => p.SourceNumber,
                (e, p) => new
                {
                    nombre = e.Name,
                    seconds = p.Seconds
                }).Where(x => x.seconds > 60).Select(x => x.nombre + " " + x.seconds + " seconds");

            foreach (var item in q1_2)
                Console.WriteLine(item);
        }

        private void Homework2()
        {
            // Show the summation, in seconds, of the phone calls done by the employees of the Computer Science department
            
            Console.WriteLine("\n\n*** *** *** HOMEWORK 2 *** *** ***\n");

            var q2 = model.Employees.Where(e => e.Department.Name.Equals("Computer Science")).Join(model.PhoneCalls,
                e => e.TelephoneNumber, p => p.SourceNumber, (e, p) => new
                {
                    name = e.Name,
                    seconds = p.Seconds
                }).GroupBy(x => x.name).Select(y => new
                {
                    nombre = y.Key,
                    sumaSegundos = y.Sum(z => z.seconds)
                });

            //.Aggregate(0, (res, e) => res += e.seconds); //SUMA

            foreach (var e in q2)
                Console.WriteLine(e);
        }

        private void Homework3()
        {
            // Show the phone calls done by each department, ordered by department names. 
            // Each line must show “Department = <Name>, Duration = <Seconds>”

            Console.WriteLine("\n\n*** *** *** HOMEWORK 3 *** *** ***\n");

            var q3 = model.PhoneCalls.Join(model.Employees, p => p.SourceNumber, e => e.TelephoneNumber,
                (p, e) => new
                {
                    department = e.Department.Name,
                    number = p.SourceNumber,
                    seconds = p.Seconds
                }).GroupBy(x => x.department).OrderBy(y => y.Key).Select(z => new
                {
                    department = z.Key,
                    duration = z.Sum(a => a.seconds)
                });

            foreach (var item in q3)
                Console.WriteLine(item);            
        }

        private void Homework4()
        {
            // Show the departments with the youngest employee, 
            // together with the name of the youngest employee and his/her age 
            // (more than one youngest employee may exist)

            Console.WriteLine("\n\n*** *** *** HOMEWORK 4 *** *** ***\n");

            var q4 = model.Departments.Join(model.Employees, d => d.Name, e => e.Department.Name,
                (d, e) => new
                {
                    department = d.Name,
                    name = e.Name,                    
                    age = d.Employees.Min(x => x.Age)
                }).GroupBy(x => x.department).OrderBy(y => y.Key);

            foreach (var item in q4)
            {
                Console.WriteLine(item.Key + ":");
                foreach(var x in item)
                    Console.WriteLine("\t" + x.name + " " + x.age);
            }


            // WITHOUT the name of the employee
            var a = model.Employees.Join(model.Departments, e => e.Department.Name, d => d.Name,
                (e, d) => new
                {
                    department = d.Name,
                    age = e.Age,
                    name = e.Name
                }).GroupBy(c => c.department).OrderBy(x => x.Key).Select(x => new
                {
                    dep = x.Key,
                    youngest = x.Min(z => z.age)
                });

            foreach (var x in a)
                Console.WriteLine("{0} ", x);
            //The YOUNGEST
            var p4 = model.Departments.Join(model.Employees, d => d.Name, e => e.Department.Name,
                (d, e) => new
                {
                    department = d.Name,
                    age = e.Age
                }).Aggregate(100, (res, e) =>
                {
                    if (res > e.age)
                        res = e.age;
                    return res;
                });
            
            Console.WriteLine("\nThe youngest age: " + p4);
        }

        private void Homework5()
        {
            // Show the greatest summation of phone call durations, in seconds, 
            // of the employees in the same department, together with the name of the department 
            // (it can be assumed that there is only one department fulfilling that condition)

            Console.WriteLine("\n\n*** *** *** HOMEWORK 5 *** *** ***\n");

            var q5 = model.Employees.Join(model.PhoneCalls, e => e.TelephoneNumber, p => p.SourceNumber,
                (e, p) => new
                {
                    secondss = p.Seconds,
                    department = e.Department.Name
                }).GroupBy(x => x.department).Select(x => new
                {
                    department = x.Key,
                    duration = x.Sum(y => y.secondss)
                });

            foreach (var item in q5)            
                Console.WriteLine(item);


            var greatest = model.Employees.Join(model.PhoneCalls, e => e.TelephoneNumber, p => p.SourceNumber,
                (e, p) => new
                {
                    department = e.Department.Name,
                    duration = p.Seconds
                }).GroupBy(x => x.department).Select(x => new
                {
                    department = x.Key,
                    max = x.Sum(z => z.duration)
                }).OrderByDescending(y => y.max);

            Console.WriteLine("\nDepartment with the greatest summation of calls: " + greatest.First());
        }
    }
}