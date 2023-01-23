using Microsoft.AspNetCore.Diagnostics;
using Microsoft.PowerBI.Api;
using Microsoft.PowerBI.Api.Models;
using Microsoft.Samples.XMLA.ExecuteQueries;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Text.Json;
using System.Threading.RateLimiting;
using System.Xml.Linq;

var configBuilder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);


IConfigurationRoot configuration = configBuilder.Build();
// Add services to the container.



var workspaces = new List<Workspace>();

configuration.Bind("Workspaces", workspaces);

foreach (var workspace in workspaces)
{
    workspace.Initialize();
}

var workspaceLookup = workspaces.ToDictionary(w => w.Id);

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRateLimiter(options =>
{
    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: httpContext.Request.QueryString.Value!,
            factory: partition => new FixedWindowRateLimiterOptions
            {
                AutoReplenishment = true,
                PermitLimit = 100,
                QueueLimit = 10,
                Window = TimeSpan.FromSeconds(10)
            }));
    options.OnRejected = (context, cancellationToken) =>
    {
        context.HttpContext.Response.StatusCode = StatusCodes.Status429TooManyRequests;
        return new ValueTask();
    };
});

builder.Services.AddSingleton(workspaceLookup);
builder.Services.AddSingleton(new ConcurrentDictionary<string, AdomdConnectionPool>());

var serOpts = new JsonSerializerOptions();
serOpts.WriteIndented = true;
serOpts.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
builder.Services.AddSingleton(serOpts);

builder.Logging.AddConsole();

var app = builder.Build();

app.UseRateLimiter();

var process = System.Diagnostics.Process.GetCurrentProcess();
int requestCount = 0;
app.Use(async (context, next) =>
{
    Interlocked.Increment(ref requestCount);
    await next(context);
});

app.Urls.Add("https://localhost:3000");
// Configure the HTTP request pipeline.

app.UseHttpsRedirection();



string sessionId = null;

app.MapGet("/HealthProbe", () => "yep");

app.MapPost("/v1.0/myorg/groups/{workspaceId:Guid}/datasets/{datasetId:Guid}/executeQueries", Handlers.ExecuteQueriesInGroup);

var appTask = app.RunAsync();

var log = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Main");

var pools = app.Services.GetService<ConcurrentDictionary<string, AdomdConnectionPool>>();

log.LogInformation("Startup complete");

while(!appTask.Wait(1000*10))
{
    process.Refresh();
    var cpuTime = process.TotalProcessorTime;
    log.LogInformation($"{pools.Count} conection pools. {requestCount} requests.  {cpuTime:c} CPU time, {requestCount/cpuTime.TotalSeconds} request per CPU Sec.");
    foreach (var (constr, pool) in pools)
    {
        
        log.LogInformation($"Pool for [{pool.RedactedConnectionString}] {pool.PoolStatusString()}");
    }

}
log.LogInformation("Shutdown complete");
