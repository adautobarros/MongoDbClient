using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.IdGenerators;

namespace MongoDbContext
{
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

        public static void UseConventionMongo()
        {
            var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("Pack", conventionPack, x => true);
        }
    }
}