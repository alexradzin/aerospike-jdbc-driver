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
                    out[gname .. '.count(*)'] = ak[gname .. '.count(*)'] + bk[gname .. '.count(*)']
                end
                if name ~= '*' then
                    if func == 'count' then
                        out[gname .. '.count(' .. name .. ')'] = ak[gname .. '.count(' .. name .. ')'] + bk[gname .. '.count(' .. name .. ')']
                    end
                    if func == 'sum' then
                        out[gname .. '.sum(' .. name .. ')'] = ak[gname .. '.sum(' .. name .. ')'] + bk[gname .. '.sum(' .. name .. ')']
                    end
                    if func == 'min' then
                        out[gname .. '.min(' .. name .. ')'] = (ak[gname .. '.min(' .. name .. ')'] > bk[gname .. '.min(' .. name .. ')'] and bk[gname .. '.min(' .. name .. ')']) or ak[gname .. '.min(' .. name .. ')']
                    end
                    if func == 'max' then
                        out[gname .. '.max(' .. name .. ')'] = (ak[gname .. '.max(' .. name .. ')'] < bk[gname .. '.max(' .. name .. ')'] and bk[gname .. '.max(' .. name .. ')']) or ak[gname .. '.max(' .. name .. ')']
                    end
                    if func == 'avg' then
                        local s = ak[gname .. '.sum(' .. name .. ')'] + bk[gname .. '.sum(' .. name .. ')']
                        local c = ak[gname .. '.count(' .. name .. ')'] + bk[gname .. '.count(' .. name .. ')']
                        out[gname .. '.avg(' .. name .. ')'] = s / c
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
                    out[gname .. '.avg(' .. name .. ')'] = out[gname .. '.sum(' .. name .. ')'] / out[gname .. '.count(' .. name .. ')']
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
            stats[gname .. '.count(*)'] = (stats[gname .. 'count(*)'] or 0) + ((rec and 1 ) or 0)
        end
        local val = ((rec and rec[name]) or nil)
        if name ~= '*' then
            info("groupby into if 1: mapper(%s)", name)
            if val then
                info("groupby into if 2: mapper(%s)", name)
                if func == 'count' or func == 'avg' then
                    stats[gname .. '.count(' .. name .. ')'] = (stats[gname .. '.count(' .. name .. ')'] or 0) + ((val and 1 ) or 0)
                end
                if func == 'sum' or func == 'avg' then
                    stats[gname .. '.sum(' .. name .. ')'] = (stats[gname .. '.sum(' .. name .. ')'] or 0) + (val or 0)
                end
                if func == 'sumsqs' then
                    stats[gname .. '.sumsqs(' .. name .. ')'] = (stats[gname .. '.sumsqs(' .. name .. ')'] or 0) + (val ^ 2)
                end
                if func == 'min' then
                    stats[gname .. '.min(' .. name .. ')'] = (not stats[gname .. '.min(' .. name .. ')'] and val) or (stats[gname .. '.min(' .. name .. ')'] and val < stats[gname .. '.min(' .. name .. ')'] and val) or stats['min(' .. name .. ')']
                end
                if func == 'max' then
                    stats[gname .. '.max(' .. name .. ')'] = (not stats[gname .. '.max(' .. name .. ')'] and val) or (stats[gname .. '.max(' .. name .. ')'] and val > stats[gname .. '.max(' .. name .. ')'] and val) or stats['max(' .. name .. ')']
                end
            end
        end
        info("groupby: mapper stats %s", stats)
        groups[grp] = stats
		return groups
	end
    return stream : map(mapper) : reduce(reducer)
end
