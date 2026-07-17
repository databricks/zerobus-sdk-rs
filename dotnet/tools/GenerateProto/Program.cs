using Databricks.Zerobus;

// ===================================================================
// Proto Schema Generator Tool
// ===================================================================
// Generates a protobuf descriptor from a Unity Catalog table JSON
// representation. This is a thin wrapper around ProtoSchema.
//
// Usage:
//   generate-proto <uc-table-json-file>
//
// The JSON file should contain the Unity Catalog API response
// for the table's GET endpoint.
// ===================================================================

if (args.Length < 1)
{
    Console.Error.WriteLine("Usage: generate-proto <uc-table-json-file>");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Generates a protobuf file descriptor from a Unity Catalog table.");
    Console.Error.WriteLine("The JSON file should contain the Unity Catalog API response");
    Console.Error.WriteLine("for the table's GET endpoint.");
    Environment.Exit(1);
}

string jsonFilePath = args[0];

if (!File.Exists(jsonFilePath))
{
    Console.Error.WriteLine($"File not found: {jsonFilePath}");
    Environment.Exit(1);
}

try
{
    string ucTableJson = File.ReadAllText(jsonFilePath);

    Console.Error.WriteLine("Generating proto schema from Unity Catalog table JSON...");

    using var schema = ProtoSchema.FromUnityCatalogJson(ucTableJson);

    byte[] descriptorBytes = schema.GetDescriptorBytes();

    // Write the descriptor proto bytes to stdout
    using var stdout = Console.OpenStandardOutput();
    stdout.Write(descriptorBytes, 0, descriptorBytes.Length);
    stdout.Flush();

    Console.Error.WriteLine($"Generated {descriptorBytes.Length} bytes of descriptor proto.");
    Console.Error.WriteLine("Write this to a .desc file or use it directly with StreamBuilder.CompiledProto().");
}
catch (ZerobusException ex)
{
    Console.Error.WriteLine($"Error generating proto schema: {ex.Message}");
    Environment.Exit(2);
}
