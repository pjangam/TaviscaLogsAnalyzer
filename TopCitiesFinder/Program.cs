using Dynamitey;
using Microsoft.CSharp.RuntimeBinder;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;


namespace TopCitiesFinder
{
    class Program
    {
        private static ConcurrentBag<KeyValuePair<string, string>> result = new ConcurrentBag<KeyValuePair<string, string>>();
        private static object dictLock = new object();
        private static object listLock = new object();


        private static int numberOfDays = 2;
        private static string elasticUri = "http://private-elasticsearch.stage.oski.io:9200/";
        //note: keys are case sensitive, values are not
        private static List<KeyValuePair<string, string>> Queries = new List<KeyValuePair<string, string>>() {
            new KeyValuePair<string, string>("AdditionalInfo.corelation-id","43058698-7c30-4520-b8b1-a1cae0e73c9b"),
            new KeyValuePair<string, string>("CallType", "usgsearchresults")
        };
        private static string reqRegex = @"(?:token=)\w+";
        private static string resRegex = "\"nextToken\":\"\\w+\"";



        static void Main(string[] args)
        {
            try
            {
                Stopwatch watch = Stopwatch.StartNew();
                int numberOfDays = 2;

                // haveing one slot of 2 hrs
                var numberOfSlots = numberOfDays * 12;

                var slots = new List<DateTime>();

                for (int i = 0; i < numberOfSlots; i++)
                {
                    slots.Add(DateTime.UtcNow.AddHours(-(2 * i)));
                }

                //foreach (var x in slots)
                Parallel.ForEach(slots, (x) =>
                {

                    ProcessLogs(x.AddHours(-2), x);
                }
                );

                Console.WriteLine($"done! Time taken {watch.ElapsedMilliseconds}");
                File.WriteAllText("C:/results.txt", JsonConvert.SerializeObject(result));
                Console.ReadKey(true);
            }
            catch (Exception ex) { Console.WriteLine(ex); }
        }

        private static void ProcessLogs(DateTime start, DateTime end)
        {

            try
            {
                var fileName = start.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture);

                var node = new Uri(elasticUri);

                var settings = new ConnectionSettings(
                    node
                ).DefaultIndex("logs-*").DisableDirectStreaming();

                var client = new ElasticClient(settings);

                var hourSlots = new List<DateTime>();

                int count = 0;
                int batchSize = 0;
                QueryContainer query = new DateRangeQuery
                {
                    Field = "Timestamp",
                    LessThan = end,
                    GreaterThan = start
                };

                foreach (var queryParameter in Queries)
                {
                    query = query && new MatchQuery
                    {
                        Field = queryParameter.Key,
                        Query = queryParameter.Value.ToLower()
                    };
                }



                bool moreResults = true;

                while (moreResults)
                {
                    var searchRequest = new SearchRequest
                    {
                        From = count,
                        Size = 200,
                        Query = query
                    };
                    count = count + 200;
                    var logsData = client.Search<Log>(searchRequest);
                    Console.WriteLine(fileName + "-" + logsData.Documents.Count);

                    if (logsData.Documents.Count == 0)
                        moreResults = false;
                    else
                        batchSize = batchSize + logsData.Documents.Count;

                    using (var webClient = new WebClient())
                    {
                        webClient.Encoding = Encoding.UTF8;
                        try
                        {
                            foreach (var logEntry in logsData.Documents)
                            {

                                try
                                {
                                    var reqString = webClient.DownloadString(logEntry.Request);

                                    var resString = webClient.DownloadString(logEntry.Response);

                                    var rqToken = Regex.Matches(reqString, reqRegex).Cast<Match>().FirstOrDefault();
                                    var rstoken = Regex.Matches(resString, resRegex).Cast<Match>().FirstOrDefault();

                                    result.Add(new KeyValuePair<string, string>(rqToken?.Value, rstoken?.Value));
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex);
                                    File.AppendAllText(@"C:\Exceptions1.txt", ex.Message + "\r\n");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                            File.AppendAllText(@"C:\Exceptions1.txt", ex.Message + "\r\n");
                        }
                    }

                    //var infofilePath = @"C:\logs\" + fileName + "-info.txt";
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                File.AppendAllText(@"C:\error.txt", ex.Message);
                throw;
            }

        }

        private static string GetQueryStringParameter(string url, string parameter)
        {
            if (!url.Contains(parameter))
                return "null";
            else
            {
                var parts = url.Split(new char[] { '?', '&' });
                foreach (var part in parts)
                {
                    var kv = part.Split('=');
                    if (kv.Length == 2 && kv[0] == parameter)
                        return kv[1];
                }
            }
            return "null";
        }

        private static bool isBodyPresent(dynamic usgRequest)
        {
            try
            {
                var x = usgRequest.Body;

                return x != null;
            }
            catch (RuntimeBinderException)
            {
                return false;
            }
        }
    }


    class Log
    {
        public string ContextIdentifier { get; set; }
        public string Response { get; set; }
        public string ApplicationName { get; set; }
        public string CallType { get; set; }
        public String Id { get; set; }
        public DateTime TimeStamp { get; set; }
        public Dictionary<string, string> AdditionalInfo { get; set; }
        public string Request { get; set; }
    }

    class Criteria
    {
        public string Checkin { get; set; }
        public string Checkout { get; set; }
        public string HotelId { get; set; }

        public string rq { get; set; }
        public string rs { get; set; }
    }
}
