using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InfluxdbClientBenchmark
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly string _host;
        private readonly string _token;
        private readonly string _org;
        private readonly string _bucket;

        public Worker(IConfiguration configuration, ILogger<Worker> logger)
        {
            _logger = logger;
            _host = configuration.GetValue<string>("Host");
            _token = configuration.GetValue<string>("Token");
            _org = configuration.GetValue<string>("Org");
            _bucket = configuration.GetValue<string>("Bucket");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using (var client = InfluxDBClientFactory.Create(_host, _token))
            {
                var orgs = await client.GetOrganizationsApi().FindOrganizationsAsync();
                var orgId = orgs.Single(o => o.Name == _org).Id;

                var bucket = await client.GetBucketsApi().FindBucketByNameAsync(_bucket);
                if (bucket != null)
                {
                    await client.GetBucketsApi().DeleteBucketAsync(bucket);
                }
                await client.GetBucketsApi().CreateBucketAsync(_bucket, orgId);
            }
            {
                using var client = InfluxDBClientFactory.Create(_host, _token);
                var writeApi = client.GetWriteApiAsync();
                var sw = new Stopwatch();
                var elapsedTimes = new List<long>();

                for (int times = 0; !stoppingToken.IsCancellationRequested; times++)
                {
                    
                    sw.Restart();

                    var tasks = Enumerable.Range(0, 11).Select(id => Task.Run(async () =>
                    {
                        var now = DateTime.UtcNow;
                        var records = Enumerable.Range(0, 200).Select(i => new RealtimeRecord
                        {
                            Time = now.AddMilliseconds(i),
                            SeismometerId = "test",
                            Z = i,
                            X = i,
                            Y = i
                        }).ToArray();
                        await writeApi.WriteMeasurementsAsync(_bucket, _org, InfluxDB.Client.Api.Domain.WritePrecision.Ms, records);
                    }));

                    await Task.WhenAll(tasks);
                    sw.Stop();
                    elapsedTimes.Add(sw.ElapsedMilliseconds);
                    _logger.LogInformation("# {times} elapsed:{elapsed}", times, sw.ElapsedMilliseconds);
                }
                _logger.LogInformation("Avg:{avg} Max:{max}", elapsedTimes.Average(), elapsedTimes.Max());
            }
        }
    }

    [Measurement(MEASUREMENT)]
    public class RealtimeRecord
    {
        public const string MEASUREMENT = "realtime_record";

        /// <summary>
        /// 資料時間
        /// </summary>
        [Column(IsTimestamp = true)]
        public DateTime Time { get; set; }

        /// <summary>
        /// 地震儀 ID
        /// </summary>
        [Column(IsTag = true)]
        public string SeismometerId { get; set; }

        /// <summary>
        /// Z方向加速度
        /// </summary>
        [Column]
        public double Z { get; set; }

        /// <summary>
        /// X方向加速度
        /// </summary>
        [Column]
        public double X { get; set; }

        /// <summary>
        /// Y方向加速度
        /// </summary>
        [Column]
        public double Y { get; set; }
    }
}
