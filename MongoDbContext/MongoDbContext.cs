using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace MongoDbContext
{
    public class MongoDbContext
    {
        private readonly Lazy<MongoClient> _lazy = new Lazy<MongoClient>(() =>
        {
            ConventionPackMongo.UseConventionMongo();
            return new MongoClient(MongoDbConfiguracao.ConnectionString);
        });

        private readonly IMongoDatabase _db;

        public MongoClient Instance => _lazy.Value;
        public MongoDbContext(string nomeBanco = "")
        {
            if (string.IsNullOrWhiteSpace(nomeBanco) && string.IsNullOrWhiteSpace(MongoDbConfiguracao.NomeBanco))
            {
                throw new InvalidOperationException("Nome do banco de dados não informado");
            }

            if (!string.IsNullOrWhiteSpace(nomeBanco))
            {
                _db = Instance.GetDatabase(nomeBanco);
            }
            else
            {
                _db = Instance.GetDatabase(MongoDbConfiguracao.NomeBanco);
            }
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
            return $"{Char.ToLowerInvariant(valor[0])}{valor.Substring(1)}";
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
            var item = await result.Project<T>(projection).FirstOrDefaultAsync();
            return item;
        }


        public async Task<T> ObterAsync<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params string[] projections)
        {
            var item = Filtrar(where, excluirId, projections);
            return await item.FirstOrDefaultAsync();
        }

        public async Task<T> ObterAsync<T>(object where, bool excluirId = true, params string[] projections)
        {
            var filtro = Filtrar<T>(where, 1, 1, excluirId: excluirId, projections: projections);
            return await filtro.FirstOrDefaultAsync();
        }

        public ICollection<T> ObterItems<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params string[] projections)
        {
            var item = Filtrar(where, excluirId, projections).ToList();
            return item;
        }

        public async Task<IList<T>> ObterItemsPorObjectAsync<T>(object where, bool excluirId = true,
            params string[] projections)
        {
            var filtros = AddFilter<T>(where);
            var projection = ObterProjection<T>(excluirId, projections);

            var itens = ObterColecao<T>()
                .Find(filtros)
                .Project<T>(projection);
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItemsPorObjectAsync<T>(object where, bool excluirId = true,
            params Expression<Func<T, Object>>[] projections)
        {
            var filtros = AddFilter<T>(where);
            var projection = ObterProjection<T>(excluirId, projections);

            var itens = ObterColecao<T>()
                .Find(filtros)
                .Project<T>(projection);
            return await itens.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItemsAsync<T>(object where, int skip = 1, int limit = 10,
            string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var item = Filtrar<T>(where, skip, limit, order, asc, excluirId, projections);
            return await item.ToListAsync();
        }

        public async Task<ICollection<T>> ObterItemsAsync<T>(Expression<Func<T, bool>> where, bool excluirId = true,
            params string[] projections)
        {
            var item = await Filtrar(where, excluirId, projections).ToListAsync();
            return item;
        }

        public async Task<ICollection<T>> ObterItemsAsync<T>(FilterDefinition<T> where, int skip = 1, int limit = 10,
            string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var item = await Filtrar(where, excluirId, projections).ToListAsync();
            return item;
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
                    filter = filter & builder.Eq(item.Key, item.Value);
                }

                i++;
            }

            return Filtrar(filter, excluirId, projections).ToList();
        }

        public async Task<ICollection<T>> ObterItemsPorFilterDefinitionAsync<T>(FilterDefinition<T> where, int skip = 1, int limit = 10,
          string order = "_id", bool asc = true, bool excluirId = true, params string[] projections)
        {
            var projection = ObterProjection<T>(excluirId, projections);

            var itens = ObterColecao<T>()
                .Find(where)
                .Skip((skip - 1) * limit)
                .Limit(limit)
                .Sort(new BsonDocument(order, asc ? 1 : -1))
                .Project<T>(projection);

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
                        filter = builder.Eq<object>(ToLowerInitLeter(name), value);
                    }
                    else
                    {
                        filter = filter & builder.Eq<object>(ToLowerInitLeter(name), value);
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

        public async Task<fieldType> ObterMaiorValorAsync<T, fieldType>(Expression<Func<T, bool>> where, Expression<Func<T, fieldType>> max)
        {
            return await ObterColecao<T>().AsQueryable().Where(where).MaxAsync(max);
        }
    }
}
