function distinct(stream, name)
	local existing = map()

    local function reducer(m1, m2)
        for k in map.keys(m2) do
            m1[k] = name
        end
        return m1
    end

	local function mapper(rec)
        local val = ((rec and rec[name]) or nil)
        existing[val] = name
		return existing
	end
    return stream : map(mapper) : reduce(reducer)
end
