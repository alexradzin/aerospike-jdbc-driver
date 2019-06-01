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

    local function join(delimiter, list)
        local result = ""
        for i=1,#list do
            if i > 1 then
                result = result .. delimiter
            end
            result = result .. list[i]
        end
        return result
    end


    -- Important: corresponding constant is defined in com.nosqldriver.sql.ResultSetOverDistinctMapFactory
    local DELIMITER = '_nsqld_as_d_'
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
                    local aggrkey = func .. '(' .. name .. ')'
                    if func == 'count' and name == '*' then
                        out[aggrkey] = ak[aggrkey] + bk[aggrkey]
                    elseif name ~= '*' then
                        if func == 'count' then
                            out[aggrkey] = ak[aggrkey] + bk[aggrkey]
                        elseif func == 'sum' then
                            out[aggrkey] = ak[aggrkey] + bk[aggrkey]
                        elseif func == 'min' then
                            out[aggrkey] = (ak[aggrkey] > bk[aggrkey] and bk[aggrkey]) or ak[aggrkey]
                        elseif func == 'max' then
                            out[aggrkey] = (ak[aggrkey] < bk[aggrkey] and bk[aggrkey]) or ak[aggrkey]
                        elseif func == 'avg' then
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
        local groupbyValues = {}
        for i=1,#groupbys do
            table.insert(groupbyValues, ((rec and rec[groupbys[i]]) or 'null'))
        end

        local grp = join(DELIMITER, groupbyValues)
        local stats = groups[grp] or map()

        for i=1,#aggrs do
            local aggr = aggrs[i]
            local func, name = split(aggr)
            local aggrkey = func .. '(' .. name .. ')'

            if func == "count" and name == "*" then
                stats[aggrkey] = (stats[aggrkey] or 0) + ((rec and 1 ) or 0)
            elseif name ~= '*' then
                local val = ((rec and rec[name]) or nil)
                local countkey = 'count(' .. name .. ')'
                local sumkey = 'sum(' .. name .. ')'
                if val then
                    if func == 'count' or func == 'avg' then
                        stats[countkey] = (stats[countkey] or 0) + ((val and 1 ) or 0)
                    elseif func == 'sum' or func == 'avg' then
                        stats[sumkey] = (stats[sumkey] or 0) + (val or 0)
                    elseif func == 'sumsqs' then
                        stats[aggrkey] = (stats[aggrkey] or 0) + (val ^ 2)
                    elseif func == 'min' then
                        stats[aggrkey] = (not stats[aggrkey] and val) or (stats[aggrkey] and val < stats[aggrkey] and val) or stats[aggrkey]
                    elseif func == 'max' then
                        stats[aggrkey] = (not stats[aggrkey] and val) or (stats[aggrkey] and val > stats[aggrkey] and val) or stats[aggrkey]
                    end
                end
            end
        end
        groups[grp] = stats
        return groups
    end

    return stream : map(mapper) : reduce(reducer)
end
