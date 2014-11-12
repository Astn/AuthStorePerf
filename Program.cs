using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.ModelConfiguration.Conventions;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reflection;
using AuthStorePerf.Properties;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.KeyspaceIsolation;
using System.Configuration;

namespace AuthStorePerf
{
    class Program 
    {

        static BenchResult SqlBench(int numberOfLogins, string connectionString, string name)
        {
            var process = name;
            var seed = 1;
            var rnd = new Random(seed);
            // Empty the table
            "{0}: Flushing DB".Write(process);
            using (var ctx = new myContext(connectionString))
            {
                ctx.Database.ExecuteSqlCommand("DELETE FROM dbo.MyLogin");
            }
            "{0}: Generating {1} logins".Write(process, numberOfLogins.ToString());
            var generatedLogins = GenerateLogins(numberOfLogins, rnd);
            // Insert the logins into the loginTable
            "{0}: Logins generated".Write(process);
            "{0}: populating database".Write(process);
            var loadTime = Stopwatch.StartNew();
            using (var ctx = new myContext(connectionString))
            {
                var wait = new AutoResetEvent(false);
                var loginsObs = generatedLogins.ToObservable()
                    .Select(
                        x =>
                            string.Format("INSERT INTO dbo.MyLogin (myKey,myValue) VALUES ('{0}','{1}');", x.myKey,
                                x.myValue))
                    .Buffer(200)
                    .Select(x => string.Join(Environment.NewLine, x));
                loginsObs.Subscribe(x =>
                {
                    ctx.Database.ExecuteSqlCommand(x);
                }
                , () =>
                {
                    wait.Set();
                });
                wait.WaitOne();
            }
            loadTime.Stop();
            "{0}: Data loaded into DB in {1}ms".Write(process, loadTime.ElapsedMilliseconds.ToString());

            var concurrentEnumerable = new System.Collections.Concurrent.BlockingCollection<string>();
            generatedLogins.ForEach(x =>
            {
                concurrentEnumerable.Add(x.myKey);
            });
            concurrentEnumerable.CompleteAdding();
            "{0}: Hashes loaded into BC".Write(process);
            findIt("", connectionString, true).Wait();
            var dbTime = Stopwatch.StartNew();
            var ctr = 0;
                    
            var l = new List<Task>();
         
            foreach (var hash in concurrentEnumerable.GetConsumingEnumerable())
            {
                ctr++;
                l.Add(findIt(hash, connectionString, false));
            }

            Task.WaitAll(l.ToArray());
                    
            dbTime.Stop();
            string.Format("{0,10}{1,12:N0}{2,12:N0}{3,12:N0}", process, ctr, dbTime.ElapsedMilliseconds, ctr / dbTime.Elapsed.TotalSeconds).Write();
            return new BenchResult { Name = process, Count = ctr, Elasped = dbTime.Elapsed };
        }

        private static async Task<bool> findIt(string hash, string connectionString, bool prepare)
        {

            using (var cxn = new SqlConnection(connectionString))
            {
                cxn.Open();
                var query =
                    string.Format(
                        "sp_executesql @statement=N'SELECT CASE WHEN EXISTS(select top 1 1 from dbo.MyLogin where myKey = @key) THEN 1 ELSE 0 END', @ParmDefinition= N'@key varchar(500)', @key = '{0}'",
                        hash);
                var cmd = new SqlCommand(query, cxn);
                cmd.CommandType = CommandType.Text;
                if(prepare)
                    cmd.Prepare();
                var result = await cmd.ExecuteScalarAsync();
                return ((int)result) == 1;
            }

        }

        static BenchResult RedisBench(int numberOfLogins, string connectionString, string name)
        {
            // generate 1 million user names and passwords
            // use the same seed to make it reproduceable.
            //var ctx = new DataClasses1DataContext();
            var process = name;
            var seed = 1;
            var rnd = new Random(seed);
            // Empty the table
            var redisConnection = ConnectionMultiplexer.Connect(connectionString);
            var db = redisConnection.GetDatabase(0);
            var endpoints = redisConnection.GetEndPoints(true);
            "{0}: Flushing DB".Write(process);
            foreach (var endpoint in endpoints)
            {
                var server = redisConnection.GetServer(endpoint);
                server.FlushDatabase();
            }
            "{0}: Generating {1} logins".Write(process, numberOfLogins.ToString());
            var generatedLogins = Enumerable
                                    .Range(1, numberOfLogins)
                                    .Select(e => new
                                    {
                                        username = string.Format("email{0}@dealersocket.com", e),
                                        password = new String(Enumerable.Range(1, 10).Select(en => (char)rnd.Next((int)'A', (int)'z')).ToArray())
                                    })
                                    .Select(e => new
                                    {
                                        e.username,
                                        e.password,
                                        loginHash = Login(e.username, e.password)
                                    })
                                    .ToList();
            // Insert the logins into the loginTable
            "{0}: Logins generated".Write(process);
            "{0}: populating database".Write(process);
            var loadTime = Stopwatch.StartNew();
            var wait = new AutoResetEvent(false);
            var loginsObs = generatedLogins.ToObservable()
               .Select(l => new KeyValuePair<RedisKey,RedisValue>
                    (
                        l.loginHash,
                        l.username
                    ))
                .Buffer(100000)
                .Select(x=>x.ToArray());

            loginsObs.Subscribe(x =>
            {
                db.StringSet(x);
            }
            , () =>
            {
                wait.Set();
            });
            wait.WaitOne();

            loadTime.Stop();
            "{0}: Data loaded into DB in {1}ms".Write(process, loadTime.ElapsedMilliseconds.ToString());
            var concurrentEnumerable = new System.Collections.Concurrent.BlockingCollection<string>();
            generatedLogins.ForEach(x =>
            {
                concurrentEnumerable.Add(x.loginHash);
            });
            concurrentEnumerable.CompleteAdding();
            "{0}: Hashes loaded into BC".Write(process);
            
            var dbTime = Stopwatch.StartNew();
            var ctr = 0;
            var innerdb = redisConnection.GetDatabase(0);
            {
                var logins = new List<Task<bool>>();
                foreach (var hash in concurrentEnumerable.GetConsumingEnumerable())
                {
                    ctr++;
                    logins.Add(innerdb.KeyExistsAsync(hash));
                }
                Task.WaitAll(logins.ToArray());
            }
            dbTime.Stop();
            string.Format("{0,10}{1,12:N0}{2,12:N0}{3,12:N0}", process, ctr, dbTime.ElapsedMilliseconds, ctr / dbTime.Elapsed.TotalSeconds).Write();
            return new BenchResult {Name=process,Count=ctr,Elasped=dbTime.Elapsed};
        }

        static void Main(params string[] args)
        {

            WriteHeader();

            var countStr = Tuple.Create("count","Number of items to insert in the database");
            var iterStr = Tuple.Create("iterations", "Number of times to run the test on each connection");
            var schemaStr = Tuple.Create("schema","Print SQL schema for a compatable table to test against");
            var configStr = Tuple.Create("useconfig", "Loads connection strings from the app.config file. Acceptable ConnectionString Proviers: (StackExchange.Redis,System.Data.SqlClient)");
            var count = 100000;
            var iterations = 2;

            var countLoc= args.ToList().IndexOf(countStr.Item1);
            var iterLoc = args.ToList().IndexOf(iterStr.Item1);
            var schemaLoc = args.ToList().IndexOf(schemaStr.Item1);

            if (args== null || args.Length == 0)
            {
                WriteCommands(countStr, iterStr, schemaStr, configStr);
                return;
            }

            if (schemaLoc >= 0)
            {
                WriteSqlSchema();
                return;
            }

            if (countLoc >= 0)
            {
                count = int.Parse(args[countLoc + 1]);
            }
            if (iterLoc >= 0)
            {
                iterations = int.Parse(args[iterLoc + 1]);
            }
            Console.WriteLine("Settings: {0}={1}, {2}={3}", countStr,count,iterStr,iterations);

            var dbConnections = new[]
            {
                new {ProviderName = "StackExchange.Redis", Name = "Redis", ConnectionString = @"localhost,allowAdmin=true"},
                new {ProviderName = "System.Data.SqlClient", Name = "Sql", ConnectionString = @"Data Source=.;Initial Catalog=LoginStoreBench;Integrated Security=True; Pooling=true;"},
                //new {ProviderName = "System.Data.SqlClient", Name = "Sql2014", ConnectionString = @"Data Source=.\SQL2014;Initial Catalog=LoginStoreBench;Integrated Security=True; Pooling=true;"},
            }.ToList();

            if (args.Contains(configStr.Item1))
            {
                dbConnections.Clear();
                for (int i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
                {
                    var con = ConfigurationManager.ConnectionStrings[i];
                    if(con.Name!="LocalSqlServer")
                        dbConnections.Add(new {ProviderName = con.ProviderName, Name=con.Name, ConnectionString=con.ConnectionString});
                }
            }
            Console.WriteLine("--Testing Connections--");
            foreach (var dbConnection in dbConnections)
            {
                Console.WriteLine("Provider={0}, Name={1}, ConnectionString={2}", dbConnection.ProviderName,dbConnection.Name, dbConnection.ConnectionString);
            }

            if (dbConnections.Any(x => x.ProviderName == "StackExchange.Redis"))
            {
                Console.WriteLine();
                var color = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("!!!!!!!WARNING!!!!!!!! ");
                Console.Write("This will FLUSH your REDIS database [YES] to continue:");
                Console.ForegroundColor = color;
                if (Console.ReadLine() != "YES")
                    return;
            }

            GC.Collect();
            Thread.Sleep(1000);
            var waitEvent = new AutoResetEvent(false);
            var results = dbConnections
                .ToObservable()
                .ObserveOn(TaskPoolScheduler.Default)
                .Repeat(iterations)
                .Do(x =>
                {
                    GC.Collect();
                    Thread.Sleep(3000);
                })
                .Select(x =>
                {
                    if (x.ProviderName == "StackExchange.Redis")
                        return new {Result = RedisBench(count, x.ConnectionString, x.Name), Connection = x};
                    if (x.ProviderName == "System.Data.SqlClient")
                        return new {Result = SqlBench(count, x.ConnectionString, x.Name), Connection = x};
                    return
                        new {Result = new BenchResult(){Name = "Missing Provider"}, Connection = x};
                })
                .ToList()
                .Subscribe(x =>
                {
                    Console.WriteLine("--RESULTS--");
                    Console.WriteLine("");
                    Console.WriteLine("{0,-10}{1,12}{2,12}{3,12}", "DB", "Count", "Elapsed(ms)", "Rate/sec");
                    Console.WriteLine("");

                    foreach (var result in from y in x
                        orderby y.Result.Elasped
                        select y)

                    {
                        Console.WriteLine("{0,10}{1,12:N0}{2,12:N0}{3,12:N0}", result.Result.Name, result.Result.Count,
                            result.Result.Elasped.TotalMilliseconds,
                            result.Result.Count/result.Result.Elasped.TotalSeconds);
                    }
                },
                    error =>
                    {
                        Console.WriteLine(error);
                    },
                    () =>
                    {
                        waitEvent.Set();
                    });
            waitEvent.WaitOne();

        }

        private static void WriteCommands(params Tuple<string,string>[] options)
        {
            Console.WriteLine("Usage:");
            
            Console.WriteLine("{0,15}\t\t{1}", "command", "description");
            Console.WriteLine();
            foreach (var option in options)
            {
                Console.WriteLine("{0,15}\t\t{1}",option.Item1,option.Item2);
            }
            Console.WriteLine();
        }

        private static void WriteHeader()
        {
            var asm = Assembly.GetEntryAssembly();
            var name = asm.GetName();
            Console.WriteLine("{0} v{1}", name.Name, name.Version);
            Console.WriteLine("CLR: {0} on: {1}", asm.ImageRuntimeVersion, name.ProcessorArchitecture);
        }

        private static void WriteSqlSchema()
        {
            Console.WriteLine(@"
Sql tests expect a schema similar to this...

USE [LoginStoreBench]
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[myLogin](
    [myKey] [varchar](500) NOT NULL,
    [myValue] [varchar](500) NOT NULL,
 CONSTRAINT [PK_Login] PRIMARY KEY CLUSTERED 
(
    [myKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
");
        }

        public static string Login(string username, string password)
        {
            var hashedPassword = CreateHash(username.ToLowerInvariant() + password);
            return string.Format("LoginStoreBench:login:{0}:{1}", username, hashedPassword);
        }
        public static string CreateHash(string unHashed)
        {
            var x = new System.Security.Cryptography.SHA1CryptoServiceProvider();
            var data = Encoding.ASCII.GetBytes(unHashed);
            data = x.ComputeHash(data);
            return Convert.ToBase64String(data);
        }

        private static List<myLogin> GenerateLogins(int numberOfLogins, Random rnd)
        {
            var generatedLogins = Enumerable
                .Range(1, numberOfLogins)
                .Select(e => new
                {
                    username = string.Format("email{0}@somedomain.com", e),
                    password = new String(Enumerable.Range(1, 10).Select(en => (char)rnd.Next((int)'A', (int)'z')).ToArray())
                })
                .Select(e => new
                {
                    e.username,
                    e.password,
                    loginHash = Login(e.username, e.password)
                })
                .Select(l => new myLogin()
                {
                    myKey = l.loginHash,
                    myValue = l.username
                })
                .ToList();
            return generatedLogins;
        }

    }

    public class BenchResult
    {
        public string Name { get; set; }
        public double Count { get; set; }


        public TimeSpan Elasped { get; set; }
    }
    public static class ext
    {
        public static string Write(this string s, params string[] args)
        {
            Console.WriteLine(s, args);
            return string.Format(s, args);
        }
    }

    [Table("myLogin")]
    public class myLogin
    {
        [Key]
        public string myKey { get; set; }
        public string myValue { get; set; }
    }

    public class myContext : DbContext
    {
        public myContext(string connectionString)
            : base(connectionString)
        {
        }

        public myContext(string connectionString, DbCompiledModel model)
            : base(connectionString, model)
        {
        }

        public DbSet<myLogin> LoginModels { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }
    }
}

