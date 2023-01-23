
using Microsoft.Extensions.Configuration;
using Microsoft.PowerBI.Api;
using Microsoft.PowerBI.Api.Models;
using Microsoft.Rest;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

var sw = new Stopwatch();

var builder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
               .AddUserSecrets("2e1a966e-6f2b-4993-a526-6c7af8f69d15");


IConfigurationRoot configuration = builder.Build();

string clientId = configuration["ClientId"];
string clientSecret = configuration["ClientSecret"];
string tenantId = configuration["TenantId"];
string groupId = configuration["GroupId"];
string datasetId = configuration["DatasetId"];
string query = configuration["Query"];
int clients = int.Parse(configuration["ClientCount"]);
int clientThinkTimeSec = int.Parse(configuration["ClientThinkTimeSec"]);

string accessToken = await Utils.GetBearerTokenAsync(clientId, clientSecret, tenantId);

var httpClient = new HttpClient();
httpClient.BaseAddress = new Uri("https://localhost:3000");
httpClient.Timeout = TimeSpan.FromMinutes(10);
httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

var tokenCredentials = new TokenCredentials(accessToken, "Bearer");
var pbiClient = new PowerBIClient(new Uri("https://localhost:3000"), tokenCredentials);

var serOpts = new JsonSerializerOptions();
serOpts.WriteIndented = true;
serOpts.MaxDepth = 10;
serOpts.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

await WaitForServerStartup(httpClient);

Console.WriteLine("Hit any key to stop testing");

bool shutdown = false;

while (!Console.KeyAvailable)
    await RunTest(groupId, datasetId, tokenCredentials, clients:clients, query:query, clientThinkTimeSec:clientThinkTimeSec);

shutdown = true;
Console.WriteLine("Complete");
async Task RunTest(string groupId, string datasetId, TokenCredentials tokenCredentials, int clients, string query, int clientThinkTimeSec)
{
    var tasks = new List<Task>();
    for (int i = 0; i < clients; i++)
    {
        Console.WriteLine("Adding worker " + i);
        
        tasks.Add(SendRequests(groupId, datasetId, tokenCredentials, 10000, $"Worker{i:d2}", query, clientThinkTimeSec, usePbiClient:false));
    }

    await Task.WhenAll(tasks.ToArray());
}

async Task SendRequests(string groupId, string datasetId, TokenCredentials tokenCredentials, int iterations, string workerName, string query, int clientThinkTimeSec, bool usePbiClient = false)
{
    Console.WriteLine($"starting worker {workerName}");
    var sw = new Stopwatch();

    var req = new DatasetExecuteQueriesRequest() { Queries = new List<DatasetExecuteQueriesQuery>() };
    req.Queries.Add(new DatasetExecuteQueriesQuery() { Query = query });
    
    for (int i = 0; i < iterations; i++)
    {
        if (shutdown)
            return;
        
        await Task.Delay((clientThinkTimeSec/2+Random.Shared.Next(1+clientThinkTimeSec)) * 1000);

        if (usePbiClient)
        {

            sw.Restart();

            Console.WriteLine($"worker {workerName} Sending request");

            try
            {
                //var pbiResponse = await pbiClient.Datasets.ExecuteQueriesInGroupWithHttpMessagesAsync(new Guid(groupId), datasetId, req);
                var pbiResponse = pbiClient.Datasets.ExecuteQueriesInGroupWithHttpMessagesAsync(new Guid(groupId), datasetId, req).Result;

                if (pbiResponse.Response.IsSuccessStatusCode)
                {
                    var br = await pbiResponse.Response.Content.ReadAsStream().CopyToBitBucket();
                    Console.WriteLine($"PowerBIClient API ExecuteQueries returned {br / 1024.0:F2}kb in {sw.Elapsed.TotalSeconds:F2}sec worker {workerName}");
                }
                else
                {
                    var body = pbiResponse.Response.Content.AsString();
                    Console.WriteLine($"PowerBIClient API ExecuteQueries failed with {(int)pbiResponse.Response.StatusCode}-{pbiResponse.Response.StatusCode} response {body}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"PowerBIClient API ExecuteQueries failed exception {ex.GetType().Name} response {ex.Message}");
            }
        }
        else
        {
            sw.Restart();

            var url = $"/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/executeQueries";
            try 
            {
                var resp = await httpClient.PostAsJsonAsync(url, req, serOpts);
                if (resp.IsSuccessStatusCode)
                {
                    var br = await resp.Content.ReadAsStream().CopyToBitBucket();
                    Console.WriteLine($"HttpClient API ExecuteQueries returned {br / 1024.0:F2}kb in {sw.Elapsed.TotalSeconds:F2}sec worker {workerName}");
                }
                else
                {
                    var body = resp.Content.AsString();
                    Console.WriteLine($"HttpClient API ExecuteQueries failed with {(int)resp.StatusCode}-{resp.StatusCode} response [{body}]");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"HttpClient API ExecuteQueries failed exception {ex.GetType().Name} response {ex.Message}");
            }
            


        }
    }
    return;

}


static async Task WaitForServerStartup(HttpClient httpClient)
{
    while (true)
    {
        try
        {
            var isAliveReq = await httpClient.GetAsync("/HealthProbe");
            if (isAliveReq.IsSuccessStatusCode)
            {
                break;
            }

        }
        catch (Exception ex)
        {
            Console.WriteLine("Waiting for server startup: " + ex.Message);
            await Task.Delay(1000);
        }
    }
}

//var reqBody = new ByteArrayContent(JsonSerializer.SerializeToUtf8Bytes(req, serOpts));
//reqBody.Headers.ContentType = new MediaTypeHeaderValue("application/json");

//sw.Restart();
//try
//{
//    var httpResp = await httpClient.PostAsync(uri, reqBody);

//    if (!httpResp.IsSuccessStatusCode)
//    {
//        Console.WriteLine($"Call to {uri} returned status code {httpResp.StatusCode}");
//        Console.WriteLine(httpResp.Content.AsString());
//        return;
//    }
//    var respBody = await httpResp.Content.ReadAsStringAsync();
//    Console.WriteLine(Utils.FormatJson(respBody));

//    var httpRespObj = JsonSerializer.Deserialize<DatasetExecuteQueriesResponse>(respBody);
//    Console.WriteLine($"Self-Hosted ExecuteQueries returned {httpRespObj.Results[0].Tables[0].Rows.Count}rows in {sw.Elapsed.TotalSeconds:D2}sec");
//}
//catch (HttpRequestException ex)
//{
//    Console.WriteLine($"{ex.StatusCode} {ex.Message}");
//    if (ex.InnerException != null)
//        Console.WriteLine(ex.InnerException.Message);
//}


