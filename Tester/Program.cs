
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

string accessToken = await Utils.GetBearerTokenAsync(clientId, clientSecret, tenantId);

var httpClient = new HttpClient();
httpClient.Timeout = TimeSpan.FromMinutes(10);

httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

var tokenCredentials = new TokenCredentials(accessToken, "Bearer");


//var pbiClient = new PowerBIClient(new Uri("https://api.powerbi.com"), tokenCredentials);
//var pbiClient = new PowerBIClient(new Uri("https://api.powerbi.com"));

var pbiClient = new PowerBIClient(new Uri("https://localhost:3000"), tokenCredentials);
//pbiClient.Reports.UpdateReportContentInGroup()
//var r = new UpdateReportContentRequest();


    

string uri = $"/v1.0/myorg/datasets/{datasetId}/executeQueries";

string xmlaEndpont = "powerbi://api.powerbi.com/v1.0/myorg/ReportExportTesting";

var datasetName = "AdventureWorksCustomerActivityReport";
//var datasetName = await Utils.GetDatasetName(groupId, datasetId, pbiClient);

httpClient.BaseAddress = new Uri("https://localhost:3000");
httpClient.DefaultRequestHeaders.Add("X-Xmla-Endpoint", xmlaEndpont);
httpClient.DefaultRequestHeaders.Add("X-Dataset-Name", datasetName);

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
        Console.WriteLine(ex.Message);
        await Task.Delay(1000);
    }
}


var req = new DatasetExecuteQueriesRequest() { Queries = new List<DatasetExecuteQueriesQuery>()};
req.Queries.Add( new DatasetExecuteQueriesQuery() { Query = "evaluate 'Internet Sales'" });

for (int i = 0; i < 10; i++)
{


    sw.Restart();
    var pbiResponse = await pbiClient.Datasets.ExecuteQueriesInGroupWithHttpMessagesAsync(new Guid(groupId),datasetId, req);
    var br = pbiResponse.Response.Content.ReadAsStream().CopyToBitBucket();
    Console.WriteLine($"PowerBI API ExecuteQueries returned {br/1024.0:F2}kb in {sw.Elapsed.TotalSeconds:F2}sec");
    //Console.WriteLine(Utils.FormatJson(body));


    //sw.Restart(); 
    //pbiResponse = await pbiClient.Datasets.ExecuteQueriesWithHttpMessagesAsync(datasetId, req);
    //br = pbiResponse.Response.Content.ReadAsStream().CopyToBitBucket();
    //Console.WriteLine($"PowerBI API ExecuteQueries returned {br / 1024.0:F2}kb in {sw.Elapsed.TotalSeconds:F2}sec");
    ////Console.WriteLine(Utils.FormatJson(body2));

}
return;

var reqBody = new ByteArrayContent(JsonSerializer.SerializeToUtf8Bytes(req,serOpts));
reqBody.Headers.ContentType = new MediaTypeHeaderValue("application/json");

sw.Restart();
try
{ 
    var httpResp = await httpClient.PostAsync(uri, reqBody);

    if (!httpResp.IsSuccessStatusCode)
    {
        Console.WriteLine($"Call to {uri} returned status code {httpResp.StatusCode}");
        Console.WriteLine(httpResp.Content.AsString());
        return;
    }
    var respBody = await httpResp.Content.ReadAsStringAsync();
    Console.WriteLine(Utils.FormatJson(respBody));
    
    var httpRespObj = JsonSerializer.Deserialize<DatasetExecuteQueriesResponse>(respBody);
    Console.WriteLine($"Self-Hosted ExecuteQueries returned {httpRespObj.Results[0].Tables[0].Rows.Count}rows in {sw.Elapsed.TotalSeconds:D2}sec");
}
catch (HttpRequestException ex)
{
    Console.WriteLine($"{ex.StatusCode} {ex.Message}");
    if (ex.InnerException != null)
        Console.WriteLine(ex.InnerException.Message);
}

