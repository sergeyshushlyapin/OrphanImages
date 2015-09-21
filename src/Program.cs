using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.Xunit2;
using Xunit;

namespace OrphanImages
{
  class Program
  {
    private static void Main(string[] args)
    {
      MainAsync(args).GetAwaiter().GetResult();

      Console.WriteLine("Done!");
      Console.ReadKey();
    }

    private async static Task MainAsync(string[] args)
    {
      var orphanCleaner = new OrphanCleaner("ecam");
      await orphanCleaner.RemoveOrphanImages();
    }
  }

  public class OrphanCleaner
  {
    public OrphanCleaner(string databaseName)
    {
      var client = new MongoClient();
      var db = client.GetDatabase(databaseName);
      Albums = db.GetCollection<Album>("albums");
      Images = db.GetCollection<Image>("images");
    }

    public IMongoCollection<Album> Albums { get; private set; }
    public IMongoCollection<Image> Images { get; private set; }

    public async Task RemoveOrphanImages()
    {
      using (var cur = await Images.FindAsync(x => true))
      {
        while (await cur.MoveNextAsync())
        {
          var batch = cur.Current;
          foreach (var image in batch)
          {
            Console.WriteLine("Checking image " + image.Id);

            var image2 = image;
            var albumImage = await Albums.Find(x => x.Images.Contains(image2.Id)).ToListAsync();
            if (albumImage.Any())
            {
              continue;
            }

            var image1 = image;
            await Images.DeleteOneAsync(x => x.Id == image1.Id);
          }
        }
      }
    }
  }

  public class CleanerTest : IDisposable
  {
    [Theory, DefaultAutoData]
    public void SutInstantiatesCollections(OrphanCleaner sut)
    {
      Assert.NotNull(sut.Albums);
      Assert.NotNull(sut.Images);
    }

    [Theory, AutoData]
    public async void RemovesOrphanImages(OrphanCleaner sut, Image image)
    {
      await sut.Images.InsertOneAsync(image);

      await sut.RemoveOrphanImages();

      var result = await sut.Images.Find("{}").ToListAsync();
      Assert.Empty(result);
    }

    [Theory, AutoData]
    public async void IgnoresImageThatAppearsInAlbum(OrphanCleaner sut, Album album, Image albumImage)
    {
      album.Images.Add(albumImage.Id);
      await sut.Images.InsertOneAsync(albumImage);
      await sut.Albums.InsertOneAsync(album);

      await sut.RemoveOrphanImages();

      var result = await sut.Images.Find("{}").ToListAsync();
      Assert.Single(result, x => x.Id == albumImage.Id);
    }

    public async void Dispose()
    {
      var client = new MongoClient();
      var db = client.GetDatabase("test");
      await db.DropCollectionAsync("images");
      await db.DropCollectionAsync("albums");
    }

    public class DefaultAutoDataAttribute : AutoDataAttribute
    {
      public DefaultAutoDataAttribute()
      {
        Fixture.Inject(new OrphanCleaner("test"));
      }
    }
  }

  [BsonIgnoreExtraElements]
  public class Album
  {
    [BsonElement("id")]
    public int Id { get; set; }

    [BsonElement("images")]
    public IList<int> Images { get; set; }
  }

  [BsonIgnoreExtraElements]
  public class Image
  {
    [BsonElement("id")]
    public int Id { get; set; }
  }
}
