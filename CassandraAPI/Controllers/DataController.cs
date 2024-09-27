using CassandraAPI.Models;
using CassandraAPI.services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.OpenApi.Any;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace CassandraAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DataController : ControllerBase
    {
        private readonly ILogger<DataController> _logger;
        private readonly CassandraService _cassandraService;
        private readonly IMemoryCache _memoryCache; // Ajout du cache

        public DataController(ILogger<DataController> logger, CassandraService cassandraService, IMemoryCache memoryCache)
        {
            _logger = logger;
            _cassandraService = cassandraService;
            _memoryCache = memoryCache;
        }

        [HttpGet("search")]
        public IActionResult SearchData(string? date, string? speedOperator, float? speed, string? densityOperator, float? density, string? btOperator, float? bt, string? bzOperator, float? bz, [Required] int pageSize = 10)
        {
            // Créer une clé unique pour le cache en fonction des paramètres
            var cacheKey = $"search-{date}-{speedOperator}-{speed}-{densityOperator}-{density}-{btOperator}-{bt}-{bzOperator}-{bz}-{pageSize}";

            // Vérifier si le résultat est déjà dans le cache
            if (_memoryCache.TryGetValue(cacheKey, out List<DataModel> cachedData))
            {
                // Retourner les données mises en cache
                return Ok(new { data = cachedData });
            }

            var query = new StringBuilder("SELECT date, speed, density, bt, bz FROM ma_table");
            if (!string.IsNullOrEmpty(date) 
                || (speed.HasValue && !string.IsNullOrEmpty(speedOperator))
                || (density.HasValue && !string.IsNullOrEmpty(densityOperator))
                || (bt.HasValue && !string.IsNullOrEmpty(btOperator))
                || (bz.HasValue && !string.IsNullOrEmpty(bzOperator)))
            {
                // 1. Construire la requête de base
                query.Append($" WHERE");

                bool hasCondition = false;

                // 2. Ajouter des filtres selon les paramètres fournis
                if (!string.IsNullOrEmpty(date))
                {
                    if (hasCondition)
                    {
                        query.Append($" AND");
                    }
                    query.Append($" date = '{date}'");
                    hasCondition = true;
                }

                if (speed.HasValue && !string.IsNullOrEmpty(speedOperator))
                {
                    if (hasCondition)
                    {
                        query.Append(" AND");
                    }
                    // Ajouter l'opérateur avant la valeur de speed
                    query.Append($" speed {speedOperator} {speed.Value}");
                    hasCondition = true;
                }

                if (density.HasValue && !string.IsNullOrEmpty(densityOperator))
                {
                    if (hasCondition)
                    {
                        query.Append(" AND");
                    }
                    query.Append($" density {densityOperator} {density}");
                    hasCondition = true;
                }

                if (bt.HasValue && !string.IsNullOrEmpty(btOperator))
                {
                    if (hasCondition)
                    {
                        query.Append(" AND");
                    }
                    query.Append($" bt {btOperator} {bt}");
                    hasCondition = true;
                }

                if (bz.HasValue && !string.IsNullOrEmpty(bzOperator))
                {
                    if (hasCondition)
                    {
                        query.Append(" AND");
                    }
                    query.Append($" bz {bzOperator} {bz}");
                    hasCondition = true;
                }
            }

            query.Append($" LIMIT {pageSize}");

            query.Append(" ALLOW FILTERING");

            try
            {
                var rows = _cassandraService.ExecuteQuery(query.ToString());

                var data = rows.Select(row => new DataModel
                {
                    Date = row.GetValue<string>("date"),
                    Speed = row.GetValue<float?>("speed"),
                    Density = row.GetValue<float?>("density"),
                    Bt = row.GetValue<float?>("bt"),
                    Bz = row.GetValue<float?>("bz")
                }).ToList();

                if (data.Count == 0)
                {
                    return NotFound();
                }

                // Ajouter les résultats au cache pour une future utilisation
                _memoryCache.Set(cacheKey, data, TimeSpan.FromMinutes(10)); // Cache de 10 minutes

                // Retourner les données récupérées avec la pagination
                return Ok(new{data});
            } catch (Exception ex)
            {
                return StatusCode(404, "Il n'existe pas de valeur");
            }
            
        }
    }
}
