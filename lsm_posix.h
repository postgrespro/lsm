#ifndef __LSM_POSIX_H__
#define __LSM_POSIX_H__

#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <semaphore.h>

#ifdef __cplusplus
extern "C" {
#endif

#define PthreadCreate(t,attr,start,arg) PthreadCreate_(t,attr,start,arg,__func__)
#define PthreadJoin(t,exitcode) PthreadJoin_(t,exitcode,__func__)
#define SemInit(sem,shared,value) SemInit_(sem,shared,value,__func__)
#define SemDestroy(sem) SemDestroy_(sem,__func__)
#define SemPost(sem) SemPost_(sem,__func__)
#define SemWait(sem) SemWait_(sem,__func__)


/*
 * Pthread
 */
extern void PthreadCreate_(pthread_t *__restrict __newthread,
						   const pthread_attr_t *__restrict __attr,
						   void *(*__start_routine)(void *),
						   void *__restrict __arg,
						   const char *fun);

extern void PthreadJoin_(pthread_t __th, void **__thread_return, const char *fun);

/*
 * Semaphore
 */
extern void SemInit_(sem_t *__sem,
					 int __pshared,
					 unsigned int __value,
					 const char *fun);

extern void SemDestroy_(sem_t *__sem, const char *fun);

extern void SemPost_(sem_t *__sem, const char *fun);

extern void SemWait_(sem_t *__sem, const char *fun);

extern void PthreadMutexLock(pthread_mutex_t* mutex);

extern void PthreadMutexUnlock(pthread_mutex_t* mutex);

extern void PthreadMutexInit(pthread_mutex_t* mutex);

extern void PthreadMutexDestroy(pthread_mutex_t* mutex);

#ifdef __cplusplus
}
#endif

#endif
