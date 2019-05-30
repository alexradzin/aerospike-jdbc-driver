function groupby(stream, ...)
    local function split(funcfield)
        local func = "count"
        local name = "*"
        local delimiterPos = string.find(funcfield, ':')
        if delimiterPos then
            func = string.sub(funcfield, 1, delimiterPos - 1)
            name = string.sub(funcfield, delimiterPos + 1, string.len(funcfield))
        end
        return func, name
    end

	local groups = map()
    local parm={...}
    local groupbys = {}
    local aggrs = {}
    for i=1,#parm do
        local func, name = split(parm[i])
        if func == 'groupby' then
            table.insert(groupbys, name)
        else
            table.insert(aggrs, parm[i])
        end
    end

    local function listtostring(list)
        local str = ""
        for i=1,#list do
            local e = (list[i] or "NIL")
            str = (str or "NIL") .. e
            if i ~= #list then
                str = (str or "NIL") .. ", "
            end
        end
        return str
    end

    local function maptostring(m)
        local str = "{"
        for k in map.keys(m) do
            str = str .. k .. "->" .. type(m[k]) .. ", "
        end
        str = str .. "}"
    end




    local function reducer(a, b)
        local result = map()


        for i=1,#aggrs do
            local aggr = aggrs[i]
            local func, name = split(aggr)
            for k in map.keys(a) do
                if b[k] then
                    local out = map()
                    local ak = a[k]
                    local bk = b[k]
                    if func == 'count' and name == '*' then
                        out['count(*)'] = ak['count(*)'] + bk['count(*)']
                    end

                    if name ~= '*' then
                        if func == 'count' then
                            out['count(' .. name .. ')'] = ak['count(' .. name .. ')'] + bk['count(' .. name .. ')']
                        end
                        if func == 'sum' then
                            out['sum(' .. name .. ')'] = ak['sum(' .. name .. ')'] + bk['sum(' .. name .. ')']
                        end
                        if func == 'min' then
                            out['min(' .. name .. ')'] = (ak['min(' .. name .. ')'] > bk['min(' .. name .. ')'] and bk['min(' .. name .. ')']) or ak['min(' .. name .. ')']
                        end
                        if func == 'max' then
                            out['max(' .. name .. ')'] = (ak['max(' .. name .. ')'] < bk['max(' .. name .. ')'] and bk['max(' .. name .. ')']) or ak['max(' .. name .. ')']
                        end
                        if func == 'avg' then
                            local s = ak['sum(' .. name .. ')'] + bk['sum(' .. name .. ')']
                            local c = ak['count(' .. name .. ')'] + bk['count(' .. name .. ')']
                            out['avg(' .. name .. ')'] = s / c
                        end
                    end
                    local rk = (result[k] or map())
                    for k1 in map.keys(out) do
                        rk[k1] = out[k1]
                    end
                    result[k] = rk
                else
                    result[k] = a[k]
                end

                for k in map.keys(b) do
                    if not a[k] then
                        result[k] = b[k]
                        local out = result[k]
                        if name ~= '*' and func == 'avg' then
                            out['avg(' .. name .. ')'] = out['sum(' .. name .. ')'] / out['count(' .. name .. ')']
                        end
                    end
                end
            end

        end

        return result
    end

	local function mapper(rec)

        local gname = groupbys[1]
        local grp = ((rec and rec[gname]) or nil)
        local stats = groups[grp] or map()

        for i=1,#aggrs do
            local aggr = aggrs[i]
            local func, name = split(aggr)

            if func == "count" and name == "*" then
                stats['count(*)'] = (stats['count(*)'] or 0) + ((rec and 1 ) or 0)
            end
            local val = ((rec and rec[name]) or nil)
            if name ~= '*' then
                if val then
                    if func == 'count' or func == 'avg' then
                        stats['count(' .. name .. ')'] = (stats['count(' .. name .. ')'] or 0) + ((val and 1 ) or 0)
                    end
                    if func == 'sum' or func == 'avg' then
                        stats['sum(' .. name .. ')'] = (stats['sum(' .. name .. ')'] or 0) + (val or 0)
                    end
                    if func == 'sumsqs' then
                        stats['sumsqs(' .. name .. ')'] = (stats['sumsqs(' .. name .. ')'] or 0) + (val ^ 2)
                    end
                    if func == 'min' then
                        stats['min(' .. name .. ')'] = (not stats['min(' .. name .. ')'] and val) or (stats['min(' .. name .. ')'] and val < stats['min(' .. name .. ')'] and val) or stats['min(' .. name .. ')']
                    end
                    if func == 'max' then
                        stats['max(' .. name .. ')'] = (not stats['max(' .. name .. ')'] and val) or (stats['max(' .. name .. ')'] and val > stats['max(' .. name .. ')'] and val) or stats['max(' .. name .. ')']
                    end
                end
            end
        end
        groups[grp] = stats
        return groups
    end

    return stream : map(mapper) : reduce(reducer)
end
