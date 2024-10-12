using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Shared;
using System.Text.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace ProductApi.ProductServices
{
    public interface IProductService
    {
        Task AddProduct(Product product);
        Task DeleteProduct(int id);
    }
    public class ProductService(IProducer<Null,string> producer) : IProductService
    {
        private List<Product> Products = [];
        public async Task AddProduct(Product product)
        {
            Products.Add(product);
            var result = await producer.ProduceAsync("add-product-topic",
                new Message<Null, string> {Value= JsonSerializer.Serialize(product) });
            if(result.Status!= PersistenceStatus.Persisted)
            {
                var lastProduct = Products.Last();
                Products.Remove(lastProduct);
            }
        }

        public async Task DeleteProduct(int id)
        {
            Products.Remove(Products.FirstOrDefault(x=>x.Id==id)!);
            await producer.ProduceAsync("delete-product-topic", new Message<Null, string>
            {
                Value = id.ToString()
            });
        }
    }
}
