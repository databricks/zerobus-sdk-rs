package com.databricks.zerobus.tools;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generate proto2 file from Unity Catalog table schema.
 *
 * <p>This tool fetches table schema from Unity Catalog and generates a corresponding proto2
 * definition file. It supports all Delta data types and maps them to appropriate Protocol Buffer
 * types.
 *
 * <p>Usage: java GenerateProto --uc-endpoint &lt;endpoint&gt; --client-id &lt;id&gt;
 * --client-secret &lt;secret&gt; --table &lt;catalog.schema.table&gt; --output &lt;output.proto&gt;
 * [--proto-msg &lt;message_name&gt;]
 *
 * <p>Type mappings: INT -&gt; int32 STRING/VARIANT -&gt; string FLOAT -&gt; float LONG/BIGINT -&gt;
 * int64 SHORT/SMALLINT -&gt; int32 DOUBLE -&gt; double BOOLEAN -&gt; bool BINARY -&gt; bytes DATE
 * -&gt; int32 TIMESTAMP -&gt; int64 ARRAY&lt;type&gt; -&gt; repeated type MAP&lt;key_type,
 * value_type&gt; -&gt; map&lt;key_type, value_type&gt;
 */
public class GenerateProto {

  private static final String USAGE =
      "Usage: java GenerateProto \n"
          + "  --uc-endpoint <endpoint>      Unity Catalog endpoint URL\n"
          + "  --client-id <id>              OAuth client ID\n"
          + "  --client-secret <secret>      OAuth client secret\n"
          + "  --table <catalog.schema.table> Full table name\n"
          + "  --output <output.proto>       Output path for proto file\n"
          + "  [--proto-msg <message_name>]  Name of protobuf message (defaults to table name)\n"
          + "\n"
          + "Examples:\n"
          + "  java GenerateProto \\\n"
          + "    --uc-endpoint \"https://your-workspace.cloud.databricks.com\" \\\n"
          + "    --client-id \"your-client-id\" \\\n"
          + "    --client-secret \"your-client-secret\" \\\n"
          + "    --table \"catalog.schema.table_name\" \\\n"
          + "    --proto-msg \"TableMessage\" \\\n"
          + "    --output \"output.proto\"\n"
          + "\n"
          + "Type mappings:\n"
          + "  Delta            -> Proto2\n"
          + "  INT              -> int32\n"
          + "  STRING           -> string\n"
          + "  VARIANT          -> string\n"
          + "  FLOAT            -> float\n"
          + "  LONG             -> int64\n"
          + "  SHORT            -> int32\n"
          + "  DOUBLE           -> double\n"
          + "  BOOLEAN          -> bool\n"
          + "  BINARY           -> bytes\n"
          + "  DATE             -> int32\n"
          + "  TIMESTAMP        -> int64\n"
          + "  ARRAY<type>      -> repeated type\n"
          + "  MAP<key_type, value_type> -> map<key_type, value_type>\n";

  public static void main(String[] args) {
    try {
      Args parsedArgs = parseArgs(args);
      run(parsedArgs);
      System.out.println("Successfully generated proto file at: " + parsedArgs.output);
      System.exit(0);
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      System.err.println();
      System.err.println(USAGE);
      System.exit(1);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void run(Args args) throws Exception {
    // Get OAuth token.
    String token = getOAuthToken(args.ucEndpoint, args.clientId, args.clientSecret);

    // Fetch table information from Unity Catalog.
    Map<String, Object> tableInfo = fetchTableInfo(args.ucEndpoint, token, args.table);

    // Extract column information.
    List<Map<String, Object>> columns = extractColumns(tableInfo);

    // Determine message name.
    String messageName = args.protoMsg != null ? args.protoMsg : args.table.split("\\.")[2];

    // Generate proto file.
    generateProtoFile(messageName, columns, args.output);
  }

  private static Args parseArgs(String[] args) {
    Args result = new Args();

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith("--")) {
        String key = arg.substring(2);
        if (i + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for argument: " + arg);
        }
        String value = args[++i];

        switch (key) {
          case "uc-endpoint":
            result.ucEndpoint = value;
            break;
          case "client-id":
            result.clientId = value;
            break;
          case "client-secret":
            result.clientSecret = value;
            break;
          case "table":
            result.table = value;
            break;
          case "output":
            result.output = value;
            break;
          case "proto-msg":
            result.protoMsg = value;
            break;
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg);
        }
      }
    }

    // Validate required arguments.
    if (result.ucEndpoint == null) {
      throw new IllegalArgumentException("Missing required argument: --uc-endpoint");
    }
    if (result.clientId == null) {
      throw new IllegalArgumentException("Missing required argument: --client-id");
    }
    if (result.clientSecret == null) {
      throw new IllegalArgumentException("Missing required argument: --client-secret");
    }
    if (result.table == null) {
      throw new IllegalArgumentException("Missing required argument: --table");
    }
    if (result.output == null) {
      throw new IllegalArgumentException("Missing required argument: --output");
    }

    return result;
  }

  /**
   * Obtains an OAuth token using client credentials flow.
   *
   * <p>This method uses basic OAuth 2.0 client credentials flow without resource or authorization
   * details.
   *
   * @param ucEndpoint The Unity Catalog endpoint URL
   * @param clientId The OAuth client ID
   * @param clientSecret The OAuth client secret
   * @return The OAuth access token (JWT)
   * @throws Exception if the token request fails
   */
  private static String getOAuthToken(String ucEndpoint, String clientId, String clientSecret)
      throws Exception {
    String urlString = ucEndpoint + "/oidc/v1/token";

    // Build OAuth 2.0 client credentials request with minimal scope.
    String formData = "grant_type=client_credentials&scope=all-apis";

    // Encode credentials for HTTP Basic authentication.
    String credentials =
        Base64.getEncoder()
            .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

    HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    connection.setRequestProperty("Authorization", "Basic " + credentials);
    connection.setDoOutput(true);

    OutputStreamWriter writer =
        new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
    writer.write(formData);
    writer.close();

    int responseCode = connection.getResponseCode();

    if (responseCode != 200) {
      String errorBody = readStream(connection.getErrorStream());
      throw new IOException("OAuth request failed with status " + responseCode + ": " + errorBody);
    }

    String responseBody = readStream(connection.getInputStream());

    // Extract access token using regex to avoid dependency on a JSON library.
    Pattern accessTokenPattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
    Matcher matcher = accessTokenPattern.matcher(responseBody);

    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new IOException("No access token received from OAuth response");
    }
  }

  /**
   * Fetch table information from Unity Catalog.
   *
   * @param endpoint Base URL of the Unity Catalog endpoint
   * @param token Authentication token
   * @param table Table identifier (catalog.schema.table)
   * @return The parsed table information as a Map
   * @throws Exception If the HTTP request fails
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> fetchTableInfo(String endpoint, String token, String table)
      throws Exception {
    String encodedTable = URLEncoder.encode(table, "UTF-8");
    String urlString = endpoint + "/api/2.1/unity-catalog/tables/" + encodedTable;

    HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", "Bearer " + token);
    connection.setRequestProperty("Content-Type", "application/json");

    int responseCode = connection.getResponseCode();

    if (responseCode != 200) {
      String errorBody = readStream(connection.getErrorStream());
      throw new IOException(
          "Failed to fetch table info with status " + responseCode + ": " + errorBody);
    }

    String responseBody = readStream(connection.getInputStream());
    return (Map<String, Object>) parseJson(responseBody);
  }

  /**
   * Extract column information from the table schema.
   *
   * @param tableInfo Raw table information from Unity Catalog
   * @return List of column information maps
   * @throws IllegalArgumentException If the expected schema structure is not found
   */
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> extractColumns(Map<String, Object> tableInfo) {
    if (!tableInfo.containsKey("columns")) {
      throw new IllegalArgumentException("No columns found in table info");
    }
    return (List<Map<String, Object>>) tableInfo.get("columns");
  }

  /**
   * Map Unity Catalog column types to proto2 field information.
   *
   * @param columnType The Unity Catalog column type
   * @param nullable Whether the column is nullable
   * @return Array containing [field_modifier, proto_type]
   * @throws IllegalArgumentException If the column type is not supported
   */
  private static String[] getProtoFieldInfo(String columnType, boolean nullable) {
    String upperType = columnType.toUpperCase();

    // Basic type mapping.
    String protoType = null;
    switch (upperType) {
      case "SMALLINT":
      case "INT":
      case "SHORT":
      case "DATE":
        protoType = "int32";
        break;
      case "BIGINT":
      case "LONG":
      case "TIMESTAMP":
        protoType = "int64";
        break;
      case "STRING":
      case "VARIANT":
        protoType = "string";
        break;
      case "FLOAT":
        protoType = "float";
        break;
      case "DOUBLE":
        protoType = "double";
        break;
      case "BOOLEAN":
        protoType = "bool";
        break;
      case "BINARY":
        protoType = "bytes";
        break;
    }

    if (protoType != null) {
      return new String[] {nullable ? "optional" : "required", protoType};
    }

    // VARCHAR types.
    if (upperType.startsWith("VARCHAR")) {
      return new String[] {nullable ? "optional" : "required", "string"};
    }

    // Array types.
    Pattern arrayPattern = Pattern.compile("^ARRAY<(.+)>$");
    Matcher arrayMatcher = arrayPattern.matcher(upperType);
    if (arrayMatcher.matches()) {
      String elementType = arrayMatcher.group(1).trim();
      String elementProtoType = getBasicProtoType(elementType);
      if (elementProtoType == null) {
        throw new IllegalArgumentException("Unsupported array element type: " + elementType);
      }
      return new String[] {"repeated", elementProtoType};
    }

    // Map types.
    Pattern mapPattern = Pattern.compile("^MAP<(.+),(.+)>$");
    Matcher mapMatcher = mapPattern.matcher(upperType);
    if (mapMatcher.matches()) {
      String keyType = mapMatcher.group(1).trim();
      String valueType = mapMatcher.group(2).trim();

      String keyProtoType = getBasicProtoType(keyType);
      if (keyProtoType == null) {
        throw new IllegalArgumentException("Unsupported map key type: " + keyType);
      }

      String valueProtoType = getBasicProtoType(valueType);
      if (valueProtoType == null) {
        throw new IllegalArgumentException("Unsupported map value type: " + valueType);
      }

      return new String[] {"", "map<" + keyProtoType + ", " + valueProtoType + ">"};
    }

    throw new IllegalArgumentException("Unsupported column type: " + columnType);
  }

  /**
   * Get basic proto type mapping for simple types.
   *
   * @param type The Unity Catalog type
   * @return The proto type or null if not a basic type
   */
  private static String getBasicProtoType(String type) {
    String upperType = type.toUpperCase();
    switch (upperType) {
      case "SMALLINT":
      case "INT":
      case "SHORT":
      case "DATE":
        return "int32";
      case "BIGINT":
      case "LONG":
      case "TIMESTAMP":
        return "int64";
      case "STRING":
      case "VARIANT":
        return "string";
      case "FLOAT":
        return "float";
      case "DOUBLE":
        return "double";
      case "BOOLEAN":
        return "bool";
      case "BINARY":
        return "bytes";
      default:
        return null;
    }
  }

  /**
   * Generate a proto2 file from the column information.
   *
   * @param messageName Name of the protobuf message
   * @param columns List of column information maps
   * @param outputPath Path where to write the proto file
   * @throws IOException If the file cannot be written
   */
  @SuppressWarnings("unchecked")
  private static void generateProtoFile(
      String messageName, List<Map<String, Object>> columns, String outputPath) throws IOException {
    StringBuilder protoContent = new StringBuilder();
    protoContent.append("syntax = \"proto2\";\n");
    protoContent.append("\n");
    protoContent.append("message ").append(messageName).append(" {\n");

    // Add fields.
    int fieldNumber = 1;
    for (Map<String, Object> col : columns) {
      // Skip protobuf reserved field numbers 19000-19999.
      if (fieldNumber == 19000) {
        fieldNumber = 20000;
      }

      String fieldName = (String) col.get("name");
      String typeText = (String) col.get("type_text");
      boolean nullable = (Boolean) col.get("nullable");

      String[] fieldInfo = getProtoFieldInfo(typeText, nullable);
      String fieldModifier = fieldInfo[0];
      String protoType = fieldInfo[1];

      if (fieldModifier.isEmpty()) {
        // Map type (no modifier).
        protoContent
            .append("    ")
            .append(protoType)
            .append(" ")
            .append(fieldName)
            .append(" = ")
            .append(fieldNumber)
            .append(";\n");
      } else {
        // Regular field or repeated field.
        protoContent
            .append("    ")
            .append(fieldModifier)
            .append(" ")
            .append(protoType)
            .append(" ")
            .append(fieldName)
            .append(" = ")
            .append(fieldNumber)
            .append(";\n");
      }
      fieldNumber++;
    }

    protoContent.append("}\n");

    // Write to file.
    try (FileWriter writer = new FileWriter(outputPath)) {
      writer.write(protoContent.toString());
    }
  }

  /** Helper method to read an input stream to a string. */
  private static String readStream(java.io.InputStream stream) throws IOException {
    if (stream == null) {
      return "No error details available";
    }
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    StringBuilder builder = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      builder.append(line).append("\n");
    }
    reader.close();
    return builder.toString();
  }

  /**
   * Simple JSON parser for basic objects and arrays. This avoids adding a JSON library dependency.
   */
  private static Object parseJson(String json) {
    return new SimpleJsonParser(json).parse();
  }

  /** Simple command-line arguments holder. */
  private static class Args {
    String ucEndpoint;
    String clientId;
    String clientSecret;
    String table;
    String output;
    String protoMsg;
  }

  /**
   * A minimal JSON parser that can handle basic objects, arrays, strings, numbers, booleans, and
   * nulls. This is sufficient for parsing Unity Catalog API responses without adding a dependency.
   */
  private static class SimpleJsonParser {
    private final String json;
    private int pos = 0;

    SimpleJsonParser(String json) {
      this.json = json.trim();
    }

    Object parse() {
      skipWhitespace();
      return parseValue();
    }

    private Object parseValue() {
      skipWhitespace();
      char c = peek();

      if (c == '{') {
        return parseObject();
      } else if (c == '[') {
        return parseArray();
      } else if (c == '"') {
        return parseString();
      } else if (c == 't' || c == 'f') {
        return parseBoolean();
      } else if (c == 'n') {
        return parseNull();
      } else if (c == '-' || Character.isDigit(c)) {
        return parseNumber();
      } else {
        throw new IllegalArgumentException("Unexpected character at position " + pos + ": " + c);
      }
    }

    private Map<String, Object> parseObject() {
      Map<String, Object> map = new java.util.HashMap<>();
      consume('{');
      skipWhitespace();

      if (peek() == '}') {
        consume('}');
        return map;
      }

      while (true) {
        skipWhitespace();
        String key = parseString();
        skipWhitespace();
        consume(':');
        skipWhitespace();
        Object value = parseValue();
        map.put(key, value);

        skipWhitespace();
        char c = peek();
        if (c == '}') {
          consume('}');
          break;
        } else if (c == ',') {
          consume(',');
        } else {
          throw new IllegalArgumentException("Expected ',' or '}' at position " + pos);
        }
      }

      return map;
    }

    private List<Object> parseArray() {
      List<Object> list = new java.util.ArrayList<>();
      consume('[');
      skipWhitespace();

      if (peek() == ']') {
        consume(']');
        return list;
      }

      while (true) {
        skipWhitespace();
        list.add(parseValue());
        skipWhitespace();

        char c = peek();
        if (c == ']') {
          consume(']');
          break;
        } else if (c == ',') {
          consume(',');
        } else {
          throw new IllegalArgumentException("Expected ',' or ']' at position " + pos);
        }
      }

      return list;
    }

    private String parseString() {
      consume('"');
      StringBuilder sb = new StringBuilder();

      while (pos < json.length()) {
        char c = json.charAt(pos);
        if (c == '"') {
          pos++;
          return sb.toString();
        } else if (c == '\\') {
          pos++;
          if (pos >= json.length()) {
            throw new IllegalArgumentException("Unterminated string escape");
          }
          char escaped = json.charAt(pos);
          switch (escaped) {
            case '"':
            case '\\':
            case '/':
              sb.append(escaped);
              break;
            case 'b':
              sb.append('\b');
              break;
            case 'f':
              sb.append('\f');
              break;
            case 'n':
              sb.append('\n');
              break;
            case 'r':
              sb.append('\r');
              break;
            case 't':
              sb.append('\t');
              break;
            case 'u':
              // Unicode escape.
              if (pos + 4 >= json.length()) {
                throw new IllegalArgumentException("Invalid unicode escape");
              }
              String hex = json.substring(pos + 1, pos + 5);
              sb.append((char) Integer.parseInt(hex, 16));
              pos += 4;
              break;
            default:
              throw new IllegalArgumentException("Invalid escape character: " + escaped);
          }
          pos++;
        } else {
          sb.append(c);
          pos++;
        }
      }

      throw new IllegalArgumentException("Unterminated string");
    }

    private Object parseNumber() {
      int start = pos;
      if (peek() == '-') {
        pos++;
      }

      while (pos < json.length()
          && (Character.isDigit(json.charAt(pos))
              || json.charAt(pos) == '.'
              || json.charAt(pos) == 'e'
              || json.charAt(pos) == 'E'
              || json.charAt(pos) == '+'
              || json.charAt(pos) == '-')) {
        pos++;
      }

      String numStr = json.substring(start, pos);
      if (numStr.contains(".") || numStr.contains("e") || numStr.contains("E")) {
        return Double.parseDouble(numStr);
      } else {
        try {
          return Integer.parseInt(numStr);
        } catch (NumberFormatException e) {
          return Long.parseLong(numStr);
        }
      }
    }

    private Boolean parseBoolean() {
      if (json.startsWith("true", pos)) {
        pos += 4;
        return Boolean.TRUE;
      } else if (json.startsWith("false", pos)) {
        pos += 5;
        return Boolean.FALSE;
      } else {
        throw new IllegalArgumentException("Invalid boolean at position " + pos);
      }
    }

    private Object parseNull() {
      if (json.startsWith("null", pos)) {
        pos += 4;
        return null;
      } else {
        throw new IllegalArgumentException("Invalid null at position " + pos);
      }
    }

    private char peek() {
      if (pos >= json.length()) {
        throw new IllegalArgumentException("Unexpected end of JSON");
      }
      return json.charAt(pos);
    }

    private void consume(char expected) {
      char c = peek();
      if (c != expected) {
        throw new IllegalArgumentException(
            "Expected '" + expected + "' but got '" + c + "' at position " + pos);
      }
      pos++;
    }

    private void skipWhitespace() {
      while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
        pos++;
      }
    }
  }
}
