#ifndef TSAN_ANNOTATION_H
#define TSAN_ANNOTATION_H

#ifdef ENABLE_THREAD_CHECK
extern "C" {
    void AnnotateHappensBefore(const char *f, int l, uintptr_t addr);
    void AnnotateHappensAfter(const char *f, int l, uintptr_t addr);
    void AnnotateRWLockCreate(char *f, int l, uintptr_t m);
    void AnnotateRWLockDestroy(char *f, int l, uintptr_t m);
    void AnnotateRWLockAcquired(char *f, int l, uintptr_t m, uintptr_t is_w);
    void AnnotateRWLockReleased(char *f, int l, uintptr_t m, uintptr_t is_w);
    void AnnotateBenignRaceSized(char *f, int l, uintptr_t m, uintptr_t size, char *desc);
    void AnnotateReadBarrier();
    void AnnotateWriteBarrier();
}
#define TsAnnotateHappensBefore(addr)      AnnotateHappensBefore(__FILE__, __LINE__, (uintptr_t)addr)
#define TsAnnotateHappensAfter(addr)       AnnotateHappensAfter(__FILE__, __LINE__, (uintptr_t)addr)
#define TsAnnotateRWLockCreate(m)          AnnotateRWLockCreate(__FILE__, __LINE__, (uintptr_t)m)
#define TsAnnotateRWLockDestroy(m)         AnnotateRWLockDestroy(__FILE__, __LINE__, (uintptr_t)m)
#define TsAnnotateRWLockAcquired(m, is_w)  AnnotateRWLockAcquired(__FILE__, __LINE__, (uintptr_t)m, is_w)
#define TsAnnotateRWLockReleased(m, is_w)  AnnotateRWLockReleased(__FILE__, __LINE__, (uintptr_t)m, is_w)
#define TsAnnotateBenignRaceSized(m, size) AnnotateBenignRaceSized(__FILE__, __LINE__, (uintptr_t)m, size, NULL)
#define TsAnnotateWriteBarrier()           AnnotateWriteBarrier()
#define TsAnnotateReadBarrier()            AnnotateReadBarrier()
#else
#define TsAnnotateHappensBefore(addr)
#define TsAnnotateHappensAfter(addr)
#define TsAnnotateRWLockCreate(m)
#define TsAnnotateRWLockDestroy(m)
#define TsAnnotateRWLockAcquired(m, is_w)
#define TsAnnotateRWLockReleased(m, is_w)
#define TsAnnotateBenignRaceSized(m, size)
#define TsAnnotateWriteBarrier()
#define TsAnnotateReadBarrier()

#endif /* endif ENABLE_THREAD_CHECK */

#endif // TSAN_ANNOTATION_H
