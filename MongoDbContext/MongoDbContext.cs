namespace MongoDbContext
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Threading.Tasks;
    using MongoDB.Bson;
    using MongoDB.Driver;

    public class MongoDbContext
    {
        private readonly IMongoDatabase _db;
        public string Query { get; set; }

        public MongoDbContext(string connectionString, string nomeBanco, bool camelCaseElementNameConvention = true, bool ignoreIfNullConvention = false, bool idGeneratorConvention = false)
        {
            if (string.IsNullOrWhiteSpace(nomeBanco) || string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException("ConnectionString não informada ou nome do banco de dados não informado");
            }


            _db = IniciarBancoComRetry(connectionString, nomeBanco, camelCaseElementNameConvention, ignoreIfNullConvention, idGeneratorConvention);

            if (_db == null)
            {
                throw new Exception("Não foi possível conectar no mongodb");
            }

        }

        private IMongoDatabase IniciarBancoComRetry(string connectionString, string nomeBanco, bool camelCaseElementNameConvention, bool ignoreIfNullConvention, bool idGeneratorConvention, int tentativas = 2)
        {
            tentativas--;

            try
            {
                var db = IniciarInstancia(connectionString, camelCaseElementNameConvention, ignoreIfNullConvention, idGeneratorConvention).GetDatabase(nomeBanco);
                if (db.Ping() && db.Client.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Connected)
                    return db;

                return IniciarBancoComRetry(connectionString, nomeBanco, camelCaseElementNameConvention, ignoreIfNullConvention, idGeneratorConvention, tentativas);

            }
            catch
            {
                if (tentativas == -1)
                {
                    throw;
                }

                if (tentativas > -1)
                    return IniciarBancoComRetry(connectionString, nomeBanco, camelCaseElementNameConvention, ignoreIfNullConvention, idGeneratorConvention, tentativas);

            }
            return null;
        }

        private MongoClient IniciarInstancia(string connectionString, bool camelCaseElementNameConvention, bool ignoreIfNullConvention, bool idGeneratorConvention)
        {
            ConventionPackMongo.UseConventionMongo(camelCaseElementNameConvention, ignoreIfNullConvention, idGeneratorConvention);
            MongoDefaults.MaxConnectionIdleTime = TimeSpan.FromMinutes(1);
            return new MongoClient(connectionString);
        }

        public bool Connectado()
        {
            return _db.Ping() && _db.Client.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Connected;
        }

        public IMongoCollection<T> ObterColecao<T>()
        {
            return _db.GetCollection<T>(ObterNomeColection<T>());
        }

        private string ObterNomeColection<T>()
        {
            var nomeColection = typeof(T).Name;
            return ToLowerInitLeter(nomeColection);
        }

        private string ToLowerInitLeter(string valor)
        {
            return $"{char.ToLowerInvariant(valor[0])}{valor.Substring(1)}";
        }

        public async Task InserirAsync<T>(T item)
        {
            await ObterColecao<T>().InsertOneAsync(item);
        }

        public async Task InserirAsync<T>(IEnumerable<T> itens)
        {
            await ObterColecao<T>().InsertManyAsync(itens);
        }

        public async Task<UpdateResult> AtualizarAsync<T>(object codigo, UpdateDefinition<T> update)
        {
            FilterDefinition<T> filtro = Builders<T>.Filter.Eq("_id", codigo);
            return await ObterColecao<T>().UpdateOneAsync(filtro, update);
        }
        public async Task<T> AtualizarAsync<T>(object codigo, T item)
        {
            FilterDefinition<T> filtro = Builders<T>.Filter.Eq("_id", codigo);
            return await ObterColecao<T>().FindOneAndReplaceAsync(filtro, item);
        }

        public async Task<T> ExcluirAsync<T>(FilterDefinition<T> filtro)
        {
            return await ObterColecao<T>().FindOneAndDeleteAsync<T>(filtro);
        }
        public async Task<DeleteResult> ExcluirAsync<T>(Expression<Func<T, bool>> where)
        {
            return await ObterColecao<T>().DeleteOneAsync<T>(where);
        }

        public async Task<UpdateResult> AtualizarAsync<T>(object codigo, object campos)
        {
            Dictionary<string, object> elementos = new Dictionary<string, object>();
            Type type = campos.GetType();
            PropertyInfo[] fields = type.GetProperties();
            for (int i = 0; i < fields.Length; i++)
            {
                var field = fields[i];
                var name = field.Name;
                var value = field.GetValue(campos, null);
                elementos.Add(ToLowerInitLeter(name), value);
            }

            return await ObterColecao<T>()
                .UpdateOneAsync(Builders<T>.Filter.Eq("_id", codigo),
                    new BsonDocument("$set", new BsonDocument(elementos)),
                    new UpdateOptions { IsUpsert = true });
        }

        public async Task<T> ObterAsync<T>(FilterDefinition<T> filter)
        {
            var result = await ObterColecao<T>()
                .FindAsync(filter);

            return result.FirstOrDefault();
        }

        public async Task<T> ObterPorIdAsync<T>(object id, bool excluirId = true, params string[] projections)
        {
            FilterDefinition<T> filtro = Builders<T>.Filter.Eq("_id", id);
            var result = ObterColecao<T>().Find(filtro);
            var projection = ObterProjection<T>(excluirId, projections);
            var item = result.Project<T>(projection);

            Query = item.ToString();
            return await item.FirstOrDefaultAsync();
        }


        public async Task<T> ObterAsync<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params string[] projections)
        {
            var item = Filtrar(where, excluirId, projections);

            Query = item.ToString();
            return await item.FirstOrDefaultAsync();
        }

        public async Task<T> ObterAsync<T>(object where, bool excluirId = true, params string[] projections)
        {
            var item = Filtrar<T>(where, 1, 1, excluirId: excluirId, projections: projections);

            Query = item.ToString();
            return await item.FirstOrDefaultAsync();
        }

        public ICollection<T> ObterItens<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params string[] projections)
        {
            var item = Filtrar(where, excluirId, projections).ToList();
            return item;
        }

        public async Task<IList<T>> ObterItensPorObjectAsync<T>(object where, bool excluirId = true,
            params string[] projections)
        {
            var filtros = AddFilter<T>(where);
            var projection = ObterProjection<T>(excluirId, projections);

            var itens = ObterColecao<T>()
                .Find(filtros)
                .Project<T>(projection);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItensPorObjectAsync<T>(object where, bool excluirId = true,
            params Expression<Func<T, Object>>[] projections)
        {
            var filtros = AddFilter<T>(where);
            var projection = ObterProjection<T>(excluirId, projections);

            var itens = ObterColecao<T>()
                .Find(filtros)
                .Project<T>(projection);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItensAsync<T>(object where, int skip = 1, int limit = 10,
            string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var itens = Filtrar<T>(where, skip, limit, order, asc, excluirId, projections);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItensAsync<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params string[] projections)
        {
            var itens = Filtrar(where, excluirId, projections);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItensAsync<T>(FilterDefinition<T> where, int skip = 1, int limit = 10,
            string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var itens = Filtrar(where, excluirId, projections);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItensPorFilterDefinitionAsync<T>(FilterDefinition<T> where, string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var projection = ObterProjection<T>(excluirId, projections);
            var itens = ObterColecao<T>()
                .Find(where)
                .Sort(new BsonDocument(order, asc ? 1 : -1))
                .Project<T>(projection);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        private IFindFluent<T, T> Filtrar<T>(Expression<Func<T, bool>> where, bool excluirId, string[] projections)
        {
            var filtro = Builders<T>.Filter.Where(where);
            var projection = ObterProjection<T>(excluirId, projections);

            var item = ObterColecao<T>()
                .Find(filtro)
                .Project<T>(projection);
            return item;
        }

        public T ObterPorExpression<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params Expression<Func<T, Object>>[] projections)
        {
            return Filtrar(where, excluirId, projections).FirstOrDefault();
        }

        public ICollection<T> ObterItensPorExpression<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params Expression<Func<T, Object>>[] projections)
        {
            return Filtrar(where, excluirId, projections).ToList();
        }

        private IFindFluent<T, T> Filtrar<T>(Expression<Func<T, bool>> where, bool excluirId,
            Expression<Func<T, object>>[] projections)
        {
            var filtro = Builders<T>.Filter.Where(where);
            var projection = ObterProjection<T>(excluirId, projections);

            var retorno = ObterColecao<T>()
                .Find(filtro)
                .Project<T>(projection);
            return retorno;
        }

        public ICollection<T> ObterItensPorFilterDefinition<T>(Dictionary<string, string> filtros,
            bool excluirId = true, params Expression<Func<T, object>>[] projections)
        {
            FilterDefinition<T> filter = FilterDefinition<T>.Empty;
            var builder = Builders<T>.Filter;

            var i = 0;
            foreach (var item in filtros)
            {
                if (i == 0)
                {
                    filter = builder.Eq(item.Key, item.Value);
                }
                else
                {
                    filter &= builder.Eq(item.Key, item.Value);
                }

                i++;
            }

            return Filtrar(filter, excluirId, projections).ToList();
        }

        public async Task<ICollection<T>> ObterItensPorFilterDefinitionAsync<T>(FilterDefinition<T> where, int skip = 1, int limit = 10,
          string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var projection = ObterProjection<T>(excluirId, projections);

            var itens = ObterColecao<T>()
                .Find(where)
                .Skip((skip - 1) * limit)
                .Limit(limit)
                .Sort(new BsonDocument(order, asc ? 1 : -1))
                .Project<T>(projection);

            Query = itens.ToString();
            return await itens.ToListAsync();
        }

        private IFindFluent<T, T> Filtrar<T>(object where = null, int skip = 1, int limit = 10, string order = "_id",
            bool asc = true, bool excluirId = true, params string[] projections)
        {
            var filtros = AddFilter<T>(where);
            var projection = ObterProjection<T>(excluirId, projections);

            return ObterColecao<T>()
                .Find(filtros)
                .Skip((skip - 1) * limit)
                .Limit(limit)
                .Sort(new BsonDocument(order, asc ? 1 : -1))
                .Project<T>(projection);
        }

        private FilterDefinition<T> AddFilter<T>(object where)
        {
            FilterDefinition<T> filter = FilterDefinition<T>.Empty;
            var builder = Builders<T>.Filter;

            int filterCount = 0;
            Type type = where.GetType();
            PropertyInfo[] fields = type.GetProperties();
            for (int i = 0; i < fields.Length; i++)
            {
                var field = fields[i];

                var name = field.Name;
                var value = field.GetValue(where, null);

                if (value != null && name != null)
                {
                    if (filterCount == 0)
                    {
                        filter = builder.Eq(ToLowerInitLeter(name), value);
                    }
                    else
                    {
                        filter &= builder.Eq(ToLowerInitLeter(name), value);
                    }

                    filterCount++;
                }

            }

            return filter;
        }

        private IFindFluent<T, T> Filtrar<T>(FilterDefinition<T> where, bool excluirId,
            Expression<Func<T, object>>[] projections)
        {
            var result = ObterColecao<T>().Find(where);
            var projection = ObterProjection<T>(excluirId, projections);

            return result.Project<T>(projection);
        }

        private IFindFluent<T, T> Filtrar<T>(FilterDefinition<T> where, bool excluirId, params string[] projections)
        {
            var result = ObterColecao<T>().Find(where);
            var projection = ObterProjection<T>(excluirId, projections);

            return result.Project<T>(projection);
        }

        private ProjectionDefinition<T> ObterProjection<T>(bool excluirId = true,
            params Expression<Func<T, Object>>[] projections)
        {
            var colunas = new List<ProjectionDefinition<T>>();
            if (projections != null && projections.Length > 0)
            {
                colunas.AddRange(projections.Select(item => Builders<T>.Projection.Include(item)));
            }

            return ObterProjection(excluirId, colunas);
        }

        private ProjectionDefinition<T> ObterProjection<T>(bool excluirId, List<ProjectionDefinition<T>> colunas)
        {
            if (excluirId)
            {
                colunas.Add(Builders<T>.Projection.Exclude("_id"));
            }

            var projection = Builders<T>.Projection.Combine(colunas.ToArray());
            return projection;
        }

        private ProjectionDefinition<T> ObterProjection<T>(bool excluirId = true, params string[] projections)
        {
            var colunas = new List<ProjectionDefinition<T>>();
            if (projections != null && projections.Length > 0)
            {
                colunas.AddRange(projections.Select(item => Builders<T>.Projection.Include(item)));
            }

            return ObterProjection(excluirId, colunas);
        }

        public async Task<long> ObterTotalDocumentosAsync<T>(FilterDefinition<T> where)
        {
            return await ObterColecao<T>().CountDocumentsAsync(where);
        }

        public async Task<RetornoPaginacao<T>> ObterQueryAsync<T>(FilterDefinition<T> filter, bool noPagination, int skip = 1, int limit = 10, string order = "_id", bool asc = true, bool excludeId = true, params string[] projections)
        {
            ICollection<T> itens;
            long totalItens;
            if (noPagination)
            {
                itens = await ObterItensPorFilterDefinitionAsync(filter, order, asc, excludeId, projections);
                totalItens = itens.Count;

            }
            else
            {
                itens = await ObterItensPorFilterDefinitionAsync(filter,
                  skip,
                  limit,
                  excluirId: excludeId,
                  order: order,
                  asc: asc,
                  projections: projections);

                totalItens = await ObterTotalDocumentosAsync(filter);
            }
            return new RetornoPaginacao<T>(itens, totalItens);
        }

    }

    public class RetornoPaginacao<T>
    {
        public RetornoPaginacao(IEnumerable<T> data, long total)
        {
            Data = data;
            Total = total;
        }

        public long Total { get; }
        public IEnumerable<T> Data { get; }
    }
}