using System.Text.Json.Serialization;

namespace Ruvio.AspNetCore.Idempotency;

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(IdempotencyMiddleware.StoredResponse))]
internal partial class IdempotencyJsonContext : JsonSerializerContext;
