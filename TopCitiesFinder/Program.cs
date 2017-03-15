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
        private static List<KeyValuePair<string, string>> tokens = new List<KeyValuePair<string, string>>();
        private static object dictLock = new object();
        private static object listLock = new object();

        static void Main(string[] args)
        {
            try
            {
                Stopwatch watch = Stopwatch.StartNew();
                // int numberOfDays = 2;

                // haveing one slot of 2 hrs
                var numberOfSlots = 2;// numberOfDays * 12;

                var slots = new List<DateTime>();

                for (int i = 0; i < numberOfSlots; i++)
                {
                    slots.Add(DateTime.UtcNow.AddHours(-(2 * i)));
                }

                foreach (var x in slots)
                //Parallel.ForEach(slots, (x) =>
                {

                    GetGeoRegionDataBetweenTimeSlots(x.AddHours(-2), x);
                }
                //);

                Console.WriteLine($"done! Time taken {watch.ElapsedMilliseconds}");
                //File.WriteAllText("C:/unmappedHotels.txt", JsonConvert.SerializeObject(errorCount));
                Console.ReadKey(true);
            }
            catch (Exception ex) { Console.WriteLine(ex); }
        }

        private static void GetGeoRegionDataBetweenTimeSlots(DateTime start, DateTime end)
        {

            try
            {
                var fileName = start.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture);

                var node = new Uri("http://private-elasticsearch.stage.oski.io:9200/");

                var settings = new ConnectionSettings(
                    node
                ).DefaultIndex("logs-*").DisableDirectStreaming();

                var client = new ElasticClient(settings);

                var hourSlots = new List<DateTime>();

                int count = 0;
                int batchSize = 0;
                QueryContainer query = new MatchQuery
                {
                    Field = "AdditionalInfo.corelation-id",
                    Query = "bd8c04f3-d4da-4ea2-9007-6a525b3a8582"
                };

                query = query && new TermQuery
                {
                    Field = "CallType",
                    Value = "usgsearchresults"
                };



                query = query && new DateRangeQuery
                {
                    Field = "Timestamp",
                    LessThan = end,
                    GreaterThan = start
                };

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
                    var response = client.Search<Log>(searchRequest);
                    Console.WriteLine(fileName + "-" + response.Documents.Count);

                    if (response.Documents.Count == 0)
                        moreResults = false;
                    else
                        batchSize = batchSize + response.Documents.Count;

                    using (var webClient = new WebClient())
                    {
                        webClient.Encoding = Encoding.UTF8;
                        try
                        {
                            foreach (var logEntry in response.Documents)
                            {

                                try
                                {
                                    var jsonStringrq = webClient.DownloadString(logEntry.Request);

                                    var jsonString = webClient.DownloadString(logEntry.Response);
                                    //var logResponse = JsonConvert.DeserializeObject<dynamic>(jsonString);

                                    var rqToken = GetQueryStringParameter(jsonStringrq, "token");
                                    var rstoken =  Regex.Matches(jsonString, "\"nextToken\":\"\\w+\"").Cast<Match>().FirstOrDefault();
                                    
                                    tokens.Add(new KeyValuePair<string, string>(rqToken,rstoken?.Value));
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

                    var infofilePath = @"C:\logs\" + fileName + "-info.txt";
                    File.WriteAllText("res.txt", JsonConvert.SerializeObject(tokens));
                    //File.AppendAllText(infofilePath, "Count=" + batchSize + "\r\n");
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
