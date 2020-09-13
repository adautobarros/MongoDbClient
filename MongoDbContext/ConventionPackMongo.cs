namespace MongoDbContext
{
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization;
    using MongoDB.Bson.Serialization.Conventions;
    using MongoDB.Bson.Serialization.IdGenerators;
    public class ConventionPackMongo
    {
        public class IdGeneratorConvention : ConventionBase, IPostProcessingConvention
        {
            public void PostProcess(BsonClassMap classMap)
            {
                var idMemberMap = classMap.IdMemberMap;
                if (idMemberMap != null && idMemberMap.MemberName == "Id" && idMemberMap.MemberType == typeof(ObjectId))
                    idMemberMap.SetIdGenerator(ObjectIdGenerator.Instance);

                if (idMemberMap != null && idMemberMap.MemberName == "Id" && idMemberMap.MemberType == typeof(string))
                    idMemberMap.SetIdGenerator(StringObjectIdGenerator.Instance);
            }
        }

        public static void UseConventionMongo(bool camelCaseElementNameConvention, bool ignoreIfNullConvention, bool idGeneratorConvention)
        {
            if (camelCaseElementNameConvention)
                ConventionRegistry.Register("camelCase", new ConventionPack { new CamelCaseElementNameConvention() }, x => true);
            if (ignoreIfNullConvention)
                ConventionRegistry.Register("Ignore null values", new ConventionPack { new IgnoreIfNullConvention(true) }, t => true);


            if (idGeneratorConvention)
                ConventionRegistry.Register("camelCase", new ConventionPack { new IdGeneratorConvention() }, x => true);
        }
    }
}