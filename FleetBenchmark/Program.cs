using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using StackExchange.Redis;
namespace FleetBenchmark
{
    [BsonIgnoreExtraElements]
    public class TelemetryRecord
    {
        public int VehicleId { get; set; }
        public DateTime Timestamp { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public double Speed { get; set; }
        public double EngineTemp { get; set; }
        public string RawData { get; set; }
    }
    class Program
    {
        const string SqlConnString = "Server=DESKTOP-L6AD50V\\SQLEXPRESS;Database=fleetmanagementsystem;Trusted_Connection=True;";
        const string MongoConnString = "mongodb://localhost:27017";
        const string RedisConnString = "localhost:6379";
        const int RecordsCount = 50000;
        const int BatchSize = 2000;
        static async Task Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;
            Console.WriteLine($"=== BENCHMARK: SQL vs NoSQL (Mongo & Redis) ===");
            Console.WriteLine($"Кількість записів: {RecordsCount}\n");
            Console.Write("Генерація даних... ");
            var data = GenerateData(RecordsCount);
            Console.WriteLine("Готово.\n");
            try
            {
                await TestSqlServer(data);
                await TestMongoDb(data);
                await TestRedis(data);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n!!! ПОМИЛКА: {ex.Message}");
                Console.WriteLine("Перевірте, чи запущені всі бази даних.");
            }
            Console.WriteLine("\nТест завершено. Натисни Enter для виходу...");
            Console.ReadLine();
        }
        static async Task TestSqlServer(List<TelemetryRecord> data)
        {
            Console.WriteLine("--- SQL SERVER (Relational) ---");
            using (var conn = new SqlConnection(SqlConnString))
            {
                await conn.OpenAsync();
                await conn.ExecuteAsync("TRUNCATE TABLE VehicleTelemetry");
                var sw = Stopwatch.StartNew();
                using (var transaction = conn.BeginTransaction())
                {
                    foreach (var batch in data.Chunk(BatchSize))
                    {
                        var sql = @"INSERT INTO VehicleTelemetry (vehicle_id, timestamp, latitude, longitude, speed, engine_temp, raw_data)
                                    VALUES (@VehicleId, @Timestamp, @Latitude, @Longitude, @Speed, @EngineTemp, @RawData)";
                        await conn.ExecuteAsync(sql, batch, transaction: transaction);
                    }
                    transaction.Commit();
                }
                sw.Stop();
                Console.WriteLine($"WRITE (Insert): {sw.ElapsedMilliseconds} ms");
                sw.Restart();
                var result = await conn.QueryAsync("SELECT * FROM VehicleTelemetry WHERE vehicle_id = @Id AND speed > 80", new { Id = 10 });
                sw.Stop();
                Console.WriteLine($"READ (Filter): {sw.ElapsedMilliseconds} ms. Знайдено: {result.Count()}");
            }
        }
        static async Task TestMongoDb(List<TelemetryRecord> data)
        {
            Console.WriteLine("\n--- MONGODB (Document NoSQL) ---");
            var client = new MongoClient(MongoConnString);
            var db = client.GetDatabase("fleet_logs");
            var collection = db.GetCollection<TelemetryRecord>("telemetry");
            await db.DropCollectionAsync("telemetry");
            var sw = Stopwatch.StartNew();
            foreach (var batch in data.Chunk(BatchSize))
            {
                await collection.InsertManyAsync(batch);
            }
            sw.Stop();
            Console.WriteLine($"WRITE (Insert): {sw.ElapsedMilliseconds} ms");
            var indexKeys = Builders<TelemetryRecord>.IndexKeys.Ascending(x => x.VehicleId).Ascending(x => x.Speed);
            await collection.Indexes.CreateOneAsync(new CreateIndexModel<TelemetryRecord>(indexKeys));
            sw.Restart();
            var filter = Builders<TelemetryRecord>.Filter.And(
                Builders<TelemetryRecord>.Filter.Eq(x => x.VehicleId, 10),
                Builders<TelemetryRecord>.Filter.Gt(x => x.Speed, 80)
            );
            var result = await collection.Find(filter).ToListAsync();
            sw.Stop();
            Console.WriteLine($"READ (Filter): {sw.ElapsedMilliseconds} ms. Знайдено: {result.Count}");
        }
        static async Task TestRedis(List<TelemetryRecord> data)
        {
            Console.WriteLine("\n--- REDIS (Key-Value NoSQL) ---");
            var redis = ConnectionMultiplexer.Connect(RedisConnString);
            var db = redis.GetDatabase();
            var sw = Stopwatch.StartNew();
            var tasks = new List<Task>();
            foreach (var record in data.Take(10000))
            {
                tasks.Add(db.StringSetAsync(
                    $"vehicle:{record.VehicleId}:pos",
                    $"{record.Latitude},{record.Longitude}"
                ));
            }
            await Task.WhenAll(tasks);
            sw.Stop();
            Console.WriteLine($"WRITE (Set Position): {sw.ElapsedMilliseconds} ms (10k операцій)");
            sw.Restart();
            var pos = await db.StringGetAsync("vehicle:10:pos");
            sw.Stop();
            Console.WriteLine($"READ (Get One): {sw.ElapsedTicks} ticks (майже 0 мс). Значення: {pos}");
        }
        static List<TelemetryRecord> GenerateData(int count)
        {
            var list = new List<TelemetryRecord>(count);
            var rnd = new Random();
            for (int i = 0; i < count; i++)
            {
                list.Add(new TelemetryRecord
                {
                    VehicleId = rnd.Next(1, 100),
                    Timestamp = DateTime.UtcNow,
                    Latitude = 50.0 + rnd.NextDouble(),
                    Longitude = 30.0 + rnd.NextDouble(),
                    Speed = rnd.Next(0, 160),
                    EngineTemp = 90 + rnd.NextDouble() * 20,
                    RawData = "{ 'sensor': 'ok' }"
                });
            }
            return list;
        }
    }
}