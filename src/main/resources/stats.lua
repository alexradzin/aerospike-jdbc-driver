function single_bin_stats(stream, ...)
	local out = map()
    parm={...}

    out['count(*)'] = 0
    for i=1,#parm do
        local name = tostring(parm[i])
        out['count(' .. name .. ')'] = 0
        out['sum(' .. name .. ')'] = 0
        out['sumsqs(' .. name .. ')'] = 0
        out['min(' .. name .. ')'] = nil
        out['max:(' .. name .. ')'] = nil
    end

    local function reducer(a, b)
        local out = map()
        out['count(*)'] = a['count(*)'] + b['count(*)']
        for i=1,#parm do
            local name = tostring(parm[i])
            out['sum(' .. name .. ')'] = a['sum(' .. name .. ')'] + b['sum(' .. name .. ')']
            out['count(' .. name .. ')'] = a['count(' .. name .. ')'] + b['count(' .. name .. ')']
            out['min(' .. name .. ')'] = (a['min(' .. name .. ')'] > b['min(' .. name .. ')'] and b['min(' .. name .. ')']) or a['min(' .. name .. ')']
            out['max(' .. name .. ')'] = (a['max(' .. name .. ')'] < b['max(' .. name .. ')'] and b['max(' .. name .. ')']) or a['max(' .. name .. ')']
            out['avg(' .. name .. ')'] = out['sum(' .. name .. ')'] / out['count(' .. name .. ')']
        end
        return out
    end

	local function mapper(rec)
        out['count(*)'] = out['count(*)'] + ((rec and 1 ) or 0)
        for i=1,#parm do
            local name = tostring(parm[i])
            local val = rec[name]
            out['sum(' .. name .. ')'] = out['sum(' .. name .. ')'] + (val or 0)
            out['count(' .. name .. ')'] = out['count(' .. name .. ')'] + ((val and 1 ) or 0)
            out['sumsqs(' .. name .. ')'] = out['sumsqs(' .. name .. ')'] + (val ^ 2)
            out['min(' .. name .. ')'] = (not out['min(' .. name .. ')'] and val) or (out['min(' .. name .. ')'] and val < out['min(' .. name .. ')'] and val) or out['min(' .. name .. ')']
            out['max(' .. name .. ')'] = (not out['max(' .. name .. ')'] and val) or (out['max(' .. name .. ')'] and val > out['max(' .. name .. ')'] and val) or out['max(' .. name .. ')']
        end
		return out
	end
	return stream : map(mapper) : reduce(reducer)
end
