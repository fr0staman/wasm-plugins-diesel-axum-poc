wasmtime::component::bindgen!({
    path: "../../wit",
    world: "plugin",
    imports: { default: async },
    // `store` is required, not stylistic: with a mix of `async func` and plain
    // `func` exports in plugin-api, async-only generates `&mut Store` calls for
    // the sync ones, which cannot be produced inside `run_concurrent`.
    exports: { default: async | store },
});
