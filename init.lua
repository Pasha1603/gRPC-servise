box.cfg { listen = 3301, memtx_memory = 2 * 1024 * 1024 * 1024 }

local kv_space = box.schema.space.create('KV', {
    if_not_exists = true,
    engine = 'memtx'
})

kv_space:format({
    {name = 'key', type = 'string'},
    {name = 'value', type = 'varbinary', is_nullable = true}
})

kv_space:create_index('primary', {
    type = 'TREE',
    parts = {'key'},
    if_not_exists = true
})

print('Tarantool KV space initialized')