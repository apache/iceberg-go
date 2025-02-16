package iceberg
import "context"
type contextKey string
const (
    RefContextKey contextKey = "ref"
)
// GetRefFromContext retrieves the ref from context
func GetRefFromContext(ctx context.Context) string {
    if ref, ok := ctx.Value(RefContextKey).(string); ok {
        return ref
    }
    return ""
}
// WithRef adds ref to context
func WithRef(ctx context.Context, ref string) context.Context {
    return context.WithValue(ctx, RefContextKey, ref)
}