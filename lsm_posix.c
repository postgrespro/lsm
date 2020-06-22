#include "lsm_posix.h"
#include "postgres.h"

void PthreadCreate_(pthread_t *__restrict __newthread,
					const pthread_attr_t *__restrict __attr,
					void *(*__start_routine)(void *),
					void *__restrict __arg,
					const char *fun) {
    int status = pthread_create(__newthread, __attr, __start_routine, __arg);
    if (status != 0) {
        ereport(ERROR, (errmsg("LSM: %s %s error number: %d", fun, __func__, status)));
    }
}

void PthreadJoin_(pthread_t __th, void **__thread_return, const char *fun) {
    int status = pthread_join(__th, __thread_return);
    if (status != 0) {
        ereport(ERROR, (errmsg("LSM: %s %s error number: %d", fun, __func__, status)));
    }
}

void SemInit_(sem_t *__sem, int __pshared, unsigned int __value, const char *fun) {
    if (sem_init(__sem, __pshared, __value) == -1) {
        ereport(ERROR, (errmsg("LSM: %s %s failed", fun, __func__)));
    }
}

void SemDestroy_(sem_t *__sem, const char *fun) {
    if (sem_destroy(__sem) == -1) {
        ereport(ERROR, (errmsg("LSM: %s %s failed", fun, __func__)));
    }
}

void SemPost_(sem_t *__sem, const char *fun) {
    if (sem_post(__sem) == -1) {
        ereport(ERROR, (errmsg("LSM: %s %s failed", fun, __func__)));
    }
}

void SemWait_(sem_t *__sem, const char *fun) {
    while (sem_wait(__sem) == -1) {
        if (errno == EINTR) {
            continue;
        }
        ereport(ERROR, (errmsg("LSM: %s %s failed", fun, __func__)));
    }
}

void PthreadMutexLock(pthread_mutex_t* mutex) {
	if (pthread_mutex_lock(mutex) != 0) {
		elog(ERROR, "LSM: failed to lock mutex: %m");
	}
}

void PthreadMutexUnlock(pthread_mutex_t* mutex) {
	if (pthread_mutex_unlock(mutex) != 0) {
		elog(ERROR, "LSM: failed to unlock mutex: %m");
	}
}

void PthreadMutexInit(pthread_mutex_t* mutex) {
	if (pthread_mutex_init(mutex, NULL) != 0) {
		elog(ERROR, "LSM: failed to create mutex: %m");
	}
}

void PthreadMutexDestroy(pthread_mutex_t* mutex) {
	if (pthread_mutex_destroy(mutex) != 0) {
		elog(ERROR, "LSM: failed to destroy mutex: %m");
	}
}
