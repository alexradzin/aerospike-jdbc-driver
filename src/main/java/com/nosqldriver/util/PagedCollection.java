package com.nosqldriver.util;

import java.util.Collection;
import java.util.Iterator;

public class PagedCollection<T> implements Collection<T> {
    private final Collection<T> collection;
    private final int pageSize;
    private int currentPageSize = 0;
    private final boolean rewrite;

    public PagedCollection(Collection<T> collection, int pageSize, boolean rewrite) {
        this.collection = collection;
        this.pageSize = pageSize;
        this.rewrite = rewrite;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return collection.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return collection.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return collection.iterator();
    }

    @Override
    public Object[] toArray() {
        return collection.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return collection.toArray(a);
    }

    @Override
    public boolean add(T t) {
        if (currentPageSize < pageSize) {
            currentPageSize++;
            return addImpl(t);
        }
        if (currentPageSize == pageSize) {
            currentPageSize = 0;
            if (rewrite) {
                collection.clear();
            }
            if (currentPageSize < pageSize) {
                currentPageSize++;
                return addImpl(t);
            }
            return false;
        }
        throw new IllegalStateException("Buffer is overloaded");
    }

    private boolean addImpl(T t) {
        return collection.add(t) && currentPageSize < pageSize;
    }


    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Paged collection does not support method remove()");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return collection.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("Paged collection does not support method addAll()");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Paged collection does not support method removeAll()");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Paged collection does not support method retainAll()");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Paged collection does not support method clear()");
    }

    @Override
    public String toString() {
        return "PagedCollection{" +
                "collection=" + collection +
                ", pageSize=" + pageSize +
                ", rewrite=" + rewrite +
                '}';
    }
}
