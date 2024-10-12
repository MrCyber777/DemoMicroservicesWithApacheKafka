using Microsoft.AspNetCore.Mvc;
using OrderApi.OrderServices;
using Shared;

namespace OrderApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class OrderController(IOrderService orderService) : ControllerBase
    {

        [HttpGet("start-consuming-service")]
        public async Task<IActionResult> StartService()
        {
            await orderService.StartConsumingService();
            return NoContent(); 
        }
        [HttpGet("get-product")]
        public async Task<IActionResult> GetProducts()
        {
            var products=orderService.GetProducts();
            return Ok(products);
        }

        [HttpPost("add-order")]
        public async Task<IActionResult> AddOrder(Order order)
        {
            orderService.AddOrder(order);
            return Ok(order);
        }
        [HttpGet("order-summary")]
        public async Task<IActionResult> GetOrdersSummary() => Ok(orderService.GetOrdersSummary());

    }
}
