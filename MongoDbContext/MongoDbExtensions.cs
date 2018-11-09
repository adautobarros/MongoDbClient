namespace MongoDbContext
{
    using MongoDB.Bson;
    using MongoDB.Driver;

    internal static class MongoDbExtensions
    {
        public static bool Ping(this IMongoDatabase db)
        {
            var resultado = db.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1)).Result;
            return resultado.ToString().Contains("ok");
        }
    }
}