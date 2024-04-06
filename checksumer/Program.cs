namespace checksumer;

public static class Program
{
    public static void Main(string[] args)
    {
        var verb = args.FirstOrDefault();
        switch (verb)
        {
            case "build":
                Environment.Exit(Build.Run(args.Skip(1).ToArray()));
                break;
            case "update":
                Environment.Exit(Update.Run(args.Skip(1).ToArray()));
                break;
            case "verify":
                Environment.Exit(Verify.Run(args.Skip(1).ToArray()));
                break;
            case null:
            {
                Console.WriteLine("No command");
                Environment.Exit(1);
                break;
            }
            default:
            {
                Console.WriteLine($"Unknown command {verb}");
                Environment.Exit(1);
                break;
            }
        }
    }
}