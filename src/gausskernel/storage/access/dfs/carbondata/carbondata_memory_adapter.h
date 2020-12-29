#ifndef CARBONDATA_MEMORY_ADAPTER_H
#define CARBONDATA_MEMORY_ADAPTER_H

#include "carbondata/memory.h"

#include "utils/memutils.h"

namespace carbondata {
class CarbondataMemoryAdapter : public MemoryMgr {
public:
    CarbondataMemoryAdapter(MemoryContext ctx) : m_ctx(ctx)
    {
        m_ctx = AllocSetContextCreate(ctx, "carbondata memory adapter", ALLOCSET_DEFAULT_MINSIZE,
                                      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }

    ~CarbondataMemoryAdapter()
    {
        MemoryContextDelete(m_ctx);
    }

    void *malloc(size_t size)
    {
        return MemoryContextAlloc(m_ctx, (Size)size);
    }
    void free(void *ptr)
    {
        FREE_POINTER(ptr);
    }

private:
    MemoryContext m_ctx;
};
}  // namespace carbondata

#endif
