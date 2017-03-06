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
using System.Threading.Tasks;

namespace TopCitiesFinder
{
    class Program
    {
        private static ConcurrentDictionary<string, int> errorCount = new ConcurrentDictionary<string, int>();
        private static object dictLock = new object();
        private static object listLock = new object();

        static void Main(string[] args)
        {
            try {
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

                     GetGeoRegionDataBetweenTimeSlots(x.AddHours(-2), x);
                 }
                );

                Console.WriteLine($"done! Time taken {watch.ElapsedMilliseconds}");
                File.WriteAllText("C:/unmappedHotels.txt", JsonConvert.SerializeObject(errorCount));
                Console.ReadKey(true); }
            catch (Exception ex) { Console.WriteLine(ex); }
        }

        private static void GetGeoRegionDataBetweenTimeSlots(DateTime start, DateTime end)
        {

            try
            {
                var fileName = start.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture);

                var node = new Uri("http://private-elasticsearch.oski.io:9200/");

                var settings = new ConnectionSettings(
                    node
                ).DefaultIndex("logs-*").DisableDirectStreaming();

                var client = new ElasticClient(settings);

                var hourSlots = new List<DateTime>();

                int count = 0;
                int batchSize = 0;
                QueryContainer query = new TermQuery
                {
                    Field = "Status",
                    Value = "failure"
                };

                query = query && new TermQuery
                {
                    Field = "CallType",
                    Value = "ean.raterules.hotelroomavailability"
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
                            foreach (var request in response.Documents)
                            {

                                try
                                {

                                    var jsonString = webClient.DownloadString(request.Response);
                                    var contentLog = JsonConvert.DeserializeObject<dynamic>(jsonString);


                                    if (isBodyPresent(contentLog))
                                    {
                                        contentLog = JsonConvert.DeserializeObject<dynamic>(Convert.ToString(contentLog.Body));
                                    }

                                    foreach (var kvp in contentLog)
                                    {
                                       
                                        var supError = kvp.Value.EanWsError.category.Value;
                                        if (errorCount.ContainsKey(supError))
                                            errorCount[supError]++;
                                        else
                                        {
                                            lock (supError)
                                            {
                                                if (!errorCount.ContainsKey(supError))
                                                {
                                                    errorCount[supError] = 1;
                                                }
                                                else errorCount[supError]++;
                                            }
                                        }

                                    }


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
    }
}
