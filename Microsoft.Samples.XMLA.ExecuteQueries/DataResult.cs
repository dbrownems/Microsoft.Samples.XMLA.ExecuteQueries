using Microsoft.AnalysisServices.AdomdClient;
using System.Net;


internal class DataResult : IResult, IDisposable
{
    private AdomdDataReader queryResults;
    private ILogger log;
    private bool gzip;
    private bool bufferResults;
    private AdomdConnection con;
    private CancellationToken cancel;
    private Action<bool> cleanup;

    public DataResult(AdomdDataReader queryResults, AdomdConnection con, bool gzip, bool bufferResults, ILogger log, Action<bool> cleanup = null)
    {
        this.cleanup = cleanup;
        this.queryResults = queryResults;
        this.log = log;
        this.gzip = gzip;
        this.bufferResults = bufferResults;
        this.con = con;
    }

    async Task WriteResults(HttpContext context)
    {


        context.Response.ContentType = "application/json";
        context.Response.StatusCode = (int)HttpStatusCode.OK;
        if (gzip)
        {
            context.Response.Headers.Add("Content-Encoding", "gzip");
        }
        var streaming = true;

        await context.Response.StartAsync();

        var responseStream = context.Response.Body;
        System.IO.Stream encodingStream = responseStream;

        if (gzip)
        {
            encodingStream = new System.IO.Compression.GZipStream(responseStream, System.IO.Compression.CompressionMode.Compress, false);
        }

        try
        {
            //streaming = false;
            if (streaming)
            {
                await queryResults.WriteAsJsonToStream(encodingStream, context.RequestAborted);
            }
            else
            {
                var ms = new MemoryStream();
                await queryResults.WriteAsJsonToStream(ms, context.RequestAborted);
                ms.Position = 0;
                var buf = new byte[256];
                ms.Read(buf, 0, buf.Length);
                var str = System.Text.Encoding.UTF8.GetString(buf);
                log.LogInformation($"buffered query results starting {str}");
                ms.Position = 0;


                await ms.CopyToAsync(encodingStream);
            }

                
            await encodingStream.FlushAsync();
            await responseStream.FlushAsync();
            await context.Response.CompleteAsync();
            if (cleanup != null)
                cleanup(true);

        }
        catch (Exception ex)
        {
            if (cleanup != null)
                cleanup(false);
            log.LogError(ex, "Error writing results");
            con.Dispose(); 
            throw;  //too late to send error to client  
        }

    }

    static System.Text.UTF8Encoding encoding = new(false);

 
    public void Dispose()
    {
        queryResults.Dispose();
        con.Dispose();
    }

    Task IResult.ExecuteAsync(HttpContext httpContext)
    {
        return this.WriteResults(httpContext);

    }
}
