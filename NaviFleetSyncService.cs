using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EO.Serwis.Portal.DataAccess;
using EO.Serwis.Portal.DataAccess.Contract.POCO;
using EO.Serwis.Portal.DataAccess.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Newtonsoft.Json.Linq;
using Serilog;

namespace NaviFleetSyncService
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class NaviFleetSyncService : StatelessService
    {
        public ConfigurationPackage ConfigPackage { get; } // odwo³anie do SF

        public NaviFleetSyncService(StatelessServiceContext context)
            : base(context)
        {
            ConfigPackage = Context.CodePackageActivationContext.GetConfigurationPackageObject("Config");

            Log.Logger = new LoggerConfiguration()
                       .WriteTo.File($@"c:\logs\EO.Portal.Payments.ServiceFabricApp\{ConfigPackage.Settings.Sections["ENV"].Parameters["Environment"].Value}\NaviFleetSyncService-.txt", rollingInterval: RollingInterval.Day)
                       .CreateLogger();

            Log.Information("Initializing NaviFleet Service!");
        }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.
            //NaviFleetClient client = new NaviFleetClient();

            long iterations = 0;

            while (true)
            {
                string eonConnectionString = $"Server={ConfigPackage.Settings.Sections["DB"].Parameters["DataSource"].Value};initial catalog={ConfigPackage.Settings.Sections["DB"].Parameters["Name"].Value};" +
                        $"user id={ConfigPackage.Settings.Sections["DB"].Parameters["Login"].Value};password={ConfigPackage.Settings.Sections["DB"].Parameters["Password"].Value};MultipleActiveResultSets=True;App=EntityFramework;";
                var opt = new Microsoft.EntityFrameworkCore.DbContextOptionsBuilder<EONDataContext>();

                using (var dataContext = new EONDataContext(opt.UseSqlServer(eonConnectionString).Options))
                {
                    var paramertySystemuRepo = new ParametrySystemuRepository(dataContext);

                    try
                    {

                        var Repo = new SamochodyLokalizacjaRepository(dataContext);//odwo³anie do repo
                        var samochodyRepo = new SamochodyRepository(dataContext);
                        var pracownicyRepo = new PracownikRepository(dataContext);

                        var wartoscParametruList = paramertySystemuRepo.Select(p => p.NazwaZmiennej == "Key").ToList(); // pobieranie wartoœci parametru key  
                        var samochody = samochodyRepo.SelectAll(); // pobieranie danych samochodu

                        Log.Information($"Pobieranie watroœci parametru zakoñczone powodzeniem");

                        foreach (var kluczAPI in wartoscParametruList)
                        {
                            using (var client = new NaviFleetClient())
                            {

                                JObject j = client.GetGPSData(kluczAPI.WartoscParametru); // pobierania danych z navifleet

                                Log.Information($"Dane GPS zosta³y pobrane");
                                var dataArray = (JArray)j["data"];

                                for (int i = 0; i < dataArray.Count; i++)
                                {
                                    var car = (JObject)dataArray[i]["device"];//przerabianie JSON na dane
                                    var carId = car.GetValue("id").Value<int>();
                                    var carName = car.GetValue("name").Value<string>();
                                    var iconColor = car.GetValue("icon_color").Value<string>();
                                    var marka = car.GetValue("mark").Value<string>();
                                    var model = car.GetValue("model").Value<string>();
                                    var licence = car.GetValue("license").Value<string>();
                                    var mileage = car.GetValue("mileage").Value<double>();
                                    // save

                                    var samochod = samochodyRepo.Select(p => p.Device_Id == carId).ToList();
                                    if (samochod.Count == 0)
                                    {
                                        var samochodyPOCO = new SamochodyPOCO()
                                        {
                                            Device_Id = carId,
                                            Device_name = carName,
                                            Device_icon_color = iconColor,
                                            Device_mark = marka,
                                            Device_model = model,
                                            Device_license = licence,
                                            Device_mileage = mileage,
                                            Data_Aktualizacji = DateTime.Now,
                                            IdPracownika = null,
                                        };
                                        samochodyRepo.Add(samochodyPOCO);
                                        samochodyRepo.Save();
                                        Log.Information($"Zapiasno dane kierowcy");
                                    }

                                    var name = samochody.Single(p => p.Device_Id == carId);
                                    if (carName != name.Device_name || iconColor != name.Device_icon_color || marka != name.Device_mark || model != name.Device_model
                                        || licence != name.Device_license || mileage != name.Device_mileage)
                                    {
                                        {
                                            name.Device_name = carName;
                                            name.Device_icon_color = iconColor;
                                            name.Device_mark = marka;
                                            name.Device_model = model;
                                            name.Device_license = licence;
                                            name.Device_mileage = mileage;
                                            name.Data_Aktualizacji = DateTime.Now;
                                        }
                                        samochodyRepo.Modify(name);
                                        samochodyRepo.Save();
                                    }

                                    var locations = (JArray)dataArray[i]["data"];
                                    for (int iter = 0; iter < locations.Count; iter++)
                                    {
                                        var gps_timestamp = ((JObject)locations[iter]).GetValue("gps_timestamp").Value<int>();
                                        var lon = ((JObject)locations[iter]).GetValue("lon").Value<double>();
                                        var lat = ((JObject)locations[iter]).GetValue("lat").Value<double>();
                                        var speed = ((JObject)locations[iter]).GetValue("speed").Value<int>();
                                        var direction = ((JObject)locations[iter]).GetValue("direction").Value<int>();
                                        var distance = ((JObject)locations[iter]).GetValue("distance").Value<int>();
                                        var totaldistance = ((JObject)locations[iter]).GetValue("total_distance").Value<double>();

                                        var auto = samochody.Single(p => p.Device_Id == carId);
                                        var poco = new SamochodyLokalizacjaPOCO() //przypisanie danych do POCO
                                        {
                                            Data_Speed = speed,
                                            Data_Total_Distance = totaldistance,
                                            Data_Direction = direction,
                                            Data_Gps_Timestamp = gps_timestamp,
                                            Data_Distance = distance,
                                            Data_lat = lat,
                                            Data_lon = lon,
                                            Data_Wpisu = DateTime.Now,
                                            IdSamochodu = auto.IdSamochodu
                                        };
                                        Repo.Add(poco);
                                        Repo.Save();
                                        Log.Information($"Zapisano lokalizacje samochodów");
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Fatal($"Podczas aktualizacji danych z GPS wyst¹pi³ b³¹d: {ex.ToString()}");
                    }
                    cancellationToken.ThrowIfCancellationRequested();

                    //ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                    var parm = paramertySystemuRepo.Select(p => p.NazwaZmiennej == "Interwal");  // pobieranie wartoœci interwa³u
                    var interwal = parm.Single();

                    await Task.Delay(TimeSpan.FromMinutes(Convert.ToDouble(interwal.WartoscParametru)), cancellationToken);
                }
            }
        }
    }
}
