
using Microsoft.Extensions.Configuration;
using Microsoft.PowerBI.Api;
using Microsoft.PowerBI.Api.Models;
using Microsoft.Rest;
using System.Diagnostics;
using System.Net.Http.Headers;
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

//string accessToken = await Utils.GetBearerTokenAsync(clientId, clientSecret, tenantId);
string accessToken = "al;dfjasofjasoifnkasjnfaslnfjasfnanfoewnwaoefnweioa";

var httpClient = new HttpClient();
httpClient.Timeout = TimeSpan.FromMinutes(10);

httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

var tokenCredentials = new TokenCredentials(accessToken, "Bearer");


//var pbiClient = new PowerBIClient(new Uri("https://api.powerbi.com"), tokenCredentials);
//var pbiClient = new PowerBIClient(new Uri("https://api.powerbi.com"));
//pbiClient.Reports.UpdateReportContentInGroup()
//var r = new UpdateReportContentRequest();




string uri = $"/v1.0/myorg/datasets/{datasetId}/executeQueries";

string xmlaEndpont = "powerbi://api.powerbi.com/v1.0/myorg/ReportExportTesting";

var datasetName = "AdventureWorksCustomerActivityReport";
//var datasetName = await Utils.GetDatasetName(groupId, datasetId, pbiClient);

httpClient.BaseAddress = new Uri("https://localhost:3000");
//httpClient.DefaultRequestHeaders.Add("X-Xmla-Endpoint", xmlaEndpont);
//httpClient.DefaultRequestHeaders.Add("X-Dataset-Name", datasetName);

var serOpts = new JsonSerializerOptions();
serOpts.WriteIndented = true;
serOpts.MaxDepth = 10;
serOpts.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

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

Console.WriteLine("Hit any key to stop testing");

while (!Console.KeyAvailable)
    await RunTest(groupId, datasetId, tokenCredentials);

Console.WriteLine("Complete");

static async Task SendRequestPBIClient(string groupId, string datasetId, TokenCredentials tokenCredentials, int iterations, string workerName)
{
    Console.WriteLine($"starting worker {workerName}");
    var sw = new Stopwatch();
    var pbiClient = new PowerBIClient(new Uri("https://localhost:3000"), tokenCredentials);

    var req = new DatasetExecuteQueriesRequest() { Queries = new List<DatasetExecuteQueriesQuery>() };
    req.Queries.Add(new DatasetExecuteQueriesQuery() { Query = "evaluate topn(100,'Internet Sales')" });

    for (int i = 0; i < iterations; i++)
    {


        sw.Restart();

        Console.WriteLine($"worker {workerName} Sending request");
        var pbiResponse = await pbiClient.Datasets.ExecuteQueriesInGroupWithHttpMessagesAsync(new Guid(groupId), datasetId, req);
        var br = await pbiResponse.Response.Content.ReadAsStream().CopyToBitBucket();

        Console.WriteLine($"PowerBI API ExecuteQueries returned {br / 1024.0:F2}kb in {sw.Elapsed.TotalSeconds:F2}sec worker {workerName}");
        //Console.WriteLine(Utils.FormatJson(body));


        //sw.Restart(); 
        //pbiResponse = await pbiClient.Datasets.ExecuteQueriesWithHttpMessagesAsync(datasetId, req);
        //br = pbiResponse.Response.Content.ReadAsStream().CopyToBitBucket();
        //Console.WriteLine($"PowerBI API ExecuteQueries returned {br / 1024.0:F2}kb in {sw.Elapsed.TotalSeconds:F2}sec");
        ////Console.WriteLine(Utils.FormatJson(body2));

    }
    return;

}

static async Task RunTest(string groupId, string datasetId, TokenCredentials tokenCredentials)
{
    var tasks = new List<Task>();
    for (int i = 0; i < 50; i++)
    {
        Console.WriteLine("Adding worker " + i);
        tasks.Add(SendRequestPBIClient(groupId, datasetId, tokenCredentials, 10, $"Worker{i:dd}"));
    }

    await Task.WhenAll(tasks.ToArray());
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


