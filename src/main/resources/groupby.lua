function groupby(stream, gname, aggr)
	local groups = map()
    local func = "count"
    local name = "*"
    local delimiterPos = string.find(aggr, ':')
    if delimiterPos then
        func = string.sub(aggr, 1, delimiterPos - 1)
        name = string.sub(aggr, delimiterPos + 1, string.len(aggr))
    end


    local function reducer(a, b)
        local result = map()
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
                result[k] = out
            else
                result[k] = a[k]
            end
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

        return result
    end

	local function mapper(rec)

        info("groupby: mapper 1 %s, %s", func, name)

        local grp = ((rec and rec[gname]) or nil)
        local stats = groups[grp] or map()
        if func == "count" and name == "*" then
            info("groupby count(*)", name)
            stats['count(*)'] = (stats['count(*)'] or 0) + ((rec and 1 ) or 0)
        end
        local val = ((rec and rec[name]) or nil)
        if name ~= '*' then
            info("groupby into if 1: mapper(%s)", name)
            if val then
                info("groupby into if 2: mapper(%s)", name)
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
        info("groupby: mapper stats %s", stats)
        groups[grp] = stats
		return groups
	end
    return stream : map(mapper) : reduce(reducer)
end
