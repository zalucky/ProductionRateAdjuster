using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Shared;
using System.Text.Json;


namespace ProductionRateAdjuster;

public class ProductionRateAdjuster
{
    private readonly ILogger _logger;
    private readonly RegistryManager _registryManager;

    public ProductionRateAdjuster(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ProductionRateAdjuster>();
        var connectionString = Environment.GetEnvironmentVariable("IOTHUB_CONNECTION");
        _registryManager = RegistryManager.CreateFromConnectionString(connectionString);
    }

    [Function("ProductionRateAdjuster")]
    public async Task RunAsync(
        [BlobTrigger("kpioutputblob/{name}", Connection = "AzureWebJobsStorage")] string blobContent,
        string name)
    {
        _logger.LogInformation($"Triggered by blob: {name}");

        using var reader = new StringReader(blobContent);
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;

            var entry = JsonSerializer.Deserialize<QualityData>(line);
            if (entry == null) continue;

            if (entry.GoodPercentage < 90)
            {
                string deviceId = entry.DeviceName.Replace(" ", "-").ToLower();
                _logger.LogWarning($"Low quality detected ({entry.GoodPercentage}%) for {deviceId}");

                try
                {
                    var twin = await _registryManager.GetTwinAsync(deviceId);
                    if (twin.Properties.Desired.Contains("ProductionRate"))
                    {
                        int currentRate = (int)twin.Properties.Desired["ProductionRate"];
                        int newRate = Math.Max(currentRate - 10, 10); // don't go below 10%

                        var patch = new TwinCollection();
                        patch["ProductionRate"] = newRate;

                        await _registryManager.UpdateTwinAsync(deviceId, new Twin() { Properties = new TwinProperties { Desired = patch } }, twin.ETag);
                        _logger.LogInformation($"Updated ProductionRate for {deviceId} from {currentRate}% to {newRate}%");
                    }
                    else
                    {
                        _logger.LogWarning($"No desired ProductionRate set for {deviceId}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error updating twin for {deviceId}: {ex.Message}");
                }
            }
        }
    }
}

public class QualityData
{
    public string DeviceName { get; set; }
    public DateTime WindowEnd { get; set; }
    public double TotalGood { get; set; }
    public double TotalProduced { get; set; }
    public double GoodPercentage { get; set; }
}
