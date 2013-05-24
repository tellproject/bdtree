#pragma once

#ifdef __cplusplus
#include <cstdint>
#include <cstring>
extern "C" {
#else
#include <stdint.h>
#include <string.h>
#endif

typedef enum {
    /// Default return value when an operation was successful.
    STATUS_OK                           = 0,

    /// Indicates that the server does not know about (and is not responsible
    /// for) a given table, but that it may exist elsewhere in the system.
    /// When it's possible that the table exists on another server, this status
    /// should be returned (in preference to the definitive TABLE_DOESNT_EXIST).
    STATUS_UNKNOWN_TABLE                = 1,

    /// Indicates that a table does not exist anywhere in the system. At present
    /// only the coordinator can say with certainly that a table does not exist.
    STATUS_TABLE_DOESNT_EXIST           = 2,


    /// Indicates that an object does not exist anywhere in the system. Note
    /// that unlike with tables there is no UNKNOWN_OBJECT status. This is just
    /// because servers will reject operations on objects in unknown tables with
    /// a table-related status. If they own a particular tablet, then they can
    /// say with certainty if an object exists there or not.
    STATUS_OBJECT_DOESNT_EXIST          = 3,

    // TODO(anyone) More documentation below, please.

    STATUS_OBJECT_EXISTS                = 4,
    STATUS_WRONG_VERSION                = 5,
    STATUS_NO_TABLE_SPACE               = 6,
    STATUS_MESSAGE_TOO_SHORT            = 7,
    STATUS_UNIMPLEMENTED_REQUEST        = 8,
    STATUS_REQUEST_FORMAT_ERROR         = 9,
    STATUS_RESPONSE_FORMAT_ERROR        = 10,
    STATUS_COULDNT_CONNECT              = 11,
    STATUS_BACKUP_BAD_SEGMENT_ID        = 12,
    STATUS_BACKUP_SEGMENT_ALREADY_OPEN  = 13,
    STATUS_BACKUP_SEGMENT_OVERFLOW      = 14,
    STATUS_BACKUP_MALFORMED_SEGMENT     = 15,
    STATUS_SEGMENT_RECOVERY_FAILED      = 16,
    STATUS_RETRY                        = 17,
    STATUS_SERVICE_NOT_AVAILABLE        = 18,
    STATUS_TIMEOUT                      = 19,
    STATUS_SERVER_DOESNT_EXIST          = 20,
    STATUS_INTERNAL_ERROR               = 21,

    /// Indicates that the object chosen for an operation does not match the
    /// associated requirements. Therefore the chosen object is invalid.
    STATUS_INVALID_OBJECT               = 22,
    /// Indicates that a tablet does not exist. This status is of relevance
    /// when doing split or merge operations on tablets are executed.
    STATUS_TABLET_DOESNT_EXIST          = 23,
    STATUS_MAX_VALUE                    = 23
} ramcloud_error_t;

typedef struct ramcloud_buffer {
    uint8_t *data;
    uint32_t length;
#ifdef __cplusplus
    ramcloud_buffer() : data(nullptr), length(0) {}
    ~ramcloud_buffer() { delete[] data; }
    void reset() {delete[] data; data = nullptr;}
#endif
} ramcloud_buffer_t;

struct ramcloud_reject_rules {
    uint64_t  givenVersion;
    uint8_t   doesntExist;
    uint8_t   exists;
    uint8_t   versionLeGiven;
    uint8_t   versionNeGiven;
#ifdef __cplusplus
    ramcloud_reject_rules()
        : givenVersion(0), doesntExist(0),
        exists(0), versionLeGiven(0), versionNeGiven(0)
    {}
#endif
};
typedef struct ramcloud_reject_rules ramcloud_reject_rules_t;

typedef uint8_t* (*alloc_ramcloud_buffer_t)(size_t);
typedef void (*dealloc_ramcloud_buffer_t)(uint8_t*);

void init_ram_cloud(const char *location, alloc_ramcloud_buffer_t alloc, dealloc_ramcloud_buffer_t dealloc);

void stop_ram_cloud();

void init_reject_rules(ramcloud_reject_rules_t *);

ramcloud_error_t rc_create_table(const char* name);
ramcloud_error_t rc_create_table_with_span(const char* name, uint32_t serverSpan);
ramcloud_error_t rc_drop_table(const char* name);
ramcloud_error_t rc_get_table_id(uint64_t *res, const char* name);
ramcloud_error_t rc_read(uint64_t tableId, const char* key, uint16_t keyLength,
                         ramcloud_buffer_t* value);
ramcloud_error_t rc_read_versioned(uint64_t tableId, const char* key, uint16_t keyLength,
                         ramcloud_buffer_t* value, uint64_t* version);

ramcloud_error_t rc_read_with_reject(uint64_t tableId, const char* key, uint16_t keyLength,
                                     ramcloud_buffer_t* value, const ramcloud_reject_rules_t* rejectRules,
                                     uint64_t* version);

ramcloud_error_t rc_increment(uint64_t tableId, const char* key, uint16_t keyLength, int64_t incrementValue);

ramcloud_error_t rc_increment_with_new(uint64_t tableId, const char* key, uint16_t keyLength,
                                       int64_t incrementValue, int64_t* newValue);

ramcloud_error_t rc_increment_with_reject(uint64_t tableId, const char* key, uint16_t keyLength,
                                          int64_t incrementValue, const ramcloud_reject_rules_t* rejectRules,
                                          uint64_t* version);

ramcloud_error_t rc_increment_with_newreject(uint64_t tableId, const char* key, uint16_t keyLength,
                                             int64_t incrementValue, const ramcloud_reject_rules_t* rejectRules,
                                             uint64_t* version, int64_t* newValue);


ramcloud_error_t rc_remove(uint64_t tableId, const char* key, uint16_t keyLength);

ramcloud_error_t rc_remove_with_reject(uint64_t tableId, const char* key, uint16_t keyLength,
                                       const ramcloud_reject_rules_t* rejectRules,
                                       uint64_t* version);

ramcloud_error_t rc_write(uint64_t tableId, const char* key, uint16_t keyLength,
                          const char* s, uint32_t len);

ramcloud_error_t rc_write_with_reject(uint64_t tableId, const char* key, uint16_t keyLength,
                                      const void* buf, uint32_t length,
                                      const ramcloud_reject_rules_t* rejectRules,
                                      uint64_t* version);

ramcloud_error_t rc_write_async(uint64_t tableId, const char* key, uint16_t keyLength,
                                const char* s, uint32_t len);

ramcloud_error_t rc_write_with_reject_async(uint64_t tableId, const char* key, uint16_t keyLength,
                                            const void* buf, uint32_t length,
                                            const ramcloud_reject_rules_t* rejectRules,
                                            uint64_t* version);

void rc_init_rand(uint32_t seed, uint32_t number_threads);

uint32_t rc_get_thread_id();

void rc_mark_done(uint32_t thread_id);

void rc_run_rand();

#ifdef __cplusplus
}
#endif
