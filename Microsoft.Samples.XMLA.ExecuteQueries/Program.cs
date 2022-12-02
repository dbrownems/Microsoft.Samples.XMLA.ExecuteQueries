using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.PowerBI.Api;
using Microsoft.PowerBI.Api.Models;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

var configBuilder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);


IConfigurationRoot configuration = configBuilder.Build();
// Add services to the container.

var app = builder.Build();

app.Urls.Add("https://localhost:3000");
// Configure the HTTP request pipeline.

app.UseHttpsRedirection();

var serOpts = new JsonSerializerOptions();
serOpts.WriteIndented = true;
serOpts.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

string sessionId = null;
AdomdConnection con = null;

app.MapGet("/HealthProbe", () => "yep");

app.MapPost("/v1.0/myorg/datasets/{datasetId:Guid}/executeQueries", DatasetExecuteQueries);

app.Run();


async Task<IResult> DatasetExecuteQueries(Guid datasetId, IConfiguration config, HttpContext context, CancellationToken cancel)
{
    
    var ms = new MemoryStream();
    await context.Request.BodyReader.CopyToAsync(ms);
    ms.Position = 0;
    var requestString = System.Text.Encoding.UTF8.GetString(ms.ToArray());


    var request = JsonSerializer.Deserialize<DatasetExecuteQueriesRequest>(requestString, serOpts);
    //var request = await JsonSerializer.DeserializeAsync<DatasetExecuteQueriesRequest>(context.Request.Body, cancellationToken: cancel);
    if (request == null)
    {
        return Results.BadRequest("DatasetExecuteQueriesRequest not found in requestBody. See https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries");
    }
    if (request.Queries.Count != 1)
    {
        return Results.BadRequest("Exactly one query must be specified");
    }
    var query = request.Queries[0].Query;

    var dataSource = config["XmlaEndpoint"];
    if (context.Request.Headers.ContainsKey("X-Xmla-Endpoint"))
    {
        dataSource = context.Request.Headers["X-Xmla-Endpoint"];
    }

    var catalog = config["DatasetName"];
    if (context.Request.Headers.ContainsKey("X-Dataset-Name"))
    {
        catalog = context.Request.Headers["X-Dataset-Name"];
    }

    if (string.IsNullOrEmpty(catalog))
    {
        return Results.BadRequest("Dataset Name must be specified in the server-side config or in the request X-Dataset-Name header");
    }

    if (string.IsNullOrEmpty(dataSource))
    {
        return Results.BadRequest("XMLA Endpoint Uri must be specified in the server-side config or in the request X-Xmla-Endpoint header");
    }


    int authHeaderCount = context.Request.Headers.Authorization.Count;
    if (authHeaderCount != 1)
    {
        return Results.Unauthorized();
    }
    var authHeader = context.Request.Headers.Authorization[0];
    if (!authHeader.StartsWith("Bearer "))
    {
        return Results.Unauthorized();
    }

    var gzip = false;
    if (context.Request.Headers.AcceptEncoding.Any(e => e.Equals("gzip", StringComparison.OrdinalIgnoreCase)))
    {
        gzip = true;
    }

    var accessToken = authHeader.Substring("Bearer ".Length);

    var constr = $"Data Source={dataSource};User Id=;Password={accessToken};Catalog={catalog};";// Persist Security Info=True; Impersonation Level=Impersonate";

    if (!string.IsNullOrEmpty(request.ImpersonatedUserName))
    {
        constr = constr + $"EffectiveUserName={request.ImpersonatedUserName}";
    }

    if (con == null)
    {
        con = new AdomdConnection(constr);
        con.Open();
    }
    
    //if (sessionId != null)
    //    con.SessionID = sessionId;
    //con.Open();

    //sessionId = con.SessionID;

   

    var cmd = con.CreateCommand();
    cmd.CommandText = query;
    var reader = cmd.ExecuteReader();

    var result = new DataResult(reader, con, gzip, false, app.Logger);
    return result;


    
}
