using Microsoft.EntityFrameworkCore;
using CustomerCommandService.Models;

namespace CustomerCommandService.Data;
public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {

    }

    public DbSet<Customer> Customers { get; set; }
}
