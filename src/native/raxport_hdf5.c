#include <hdf5.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RAX_OK 0
#define RAX_FAIL 1
#define SCAN_CHUNK 60000
#define PEAK_CHUNK 262144
#define SCAN_FILTER_STRING_SIZE 1024
#define ACTIVATION_STRING_SIZE 64

#ifdef _WIN32
#define RAXPORT_API __declspec(dllexport)
#else
#define RAXPORT_API __attribute__((visibility("default")))
#endif

typedef struct RaxH5Writer {
    hid_t file;
    hid_t scans;
    hid_t peaks;
    hid_t peak_mobility_traces;
    hid_t reactions;
    hid_t candidates;
    hid_t string_tables;

    hid_t scan_number;
    hid_t ms_order;
    hid_t retention_time;
    hid_t tic;
    hid_t scan_filter_id;
    hid_t activation_id;
    hid_t parent_scan_number;
    hid_t reaction_start;
    hid_t reaction_count;
    hid_t peak_start;
    hid_t peak_count;

    hid_t peak_mz;
    hid_t peak_intensity;
    hid_t peak_resolution;
    hid_t peak_baseline;
    hid_t peak_noise;
    hid_t peak_charge;
    hid_t peak_mobility_trace_start;
    hid_t peak_mobility_trace_count;

    hid_t trace_one_over_k0_index;
    hid_t trace_intensity;

    hid_t reaction_precursor_mass;
    hid_t reaction_isolation_width;
    hid_t reaction_charge_state;
    hid_t reaction_collision_energy;
    hid_t reaction_collision_energy_valid;
    hid_t reaction_activation_type_id;
    hid_t reaction_multiple_activation;
    hid_t reaction_precursor_range_valid;
    hid_t reaction_first_precursor_mass;
    hid_t reaction_last_precursor_mass;
    hid_t reaction_isolation_width_offset;
    hid_t reaction_one_over_k0_begin;
    hid_t reaction_one_over_k0_end;
    hid_t reaction_candidate_start;
    hid_t reaction_candidate_count;

    hid_t candidate_charge;
    hid_t candidate_mz;
    hid_t candidate_intensity;
    hid_t candidate_one_over_k0;

    hid_t scan_filter_value;
    hid_t activation_value;
    hid_t reaction_activation_type_value;

    hsize_t scan_rows;
    hsize_t peak_rows;
    hsize_t trace_rows;
    hsize_t reaction_rows;
    hsize_t candidate_rows;
    hsize_t scan_filter_rows;
    hsize_t activation_rows;
    hsize_t reaction_activation_type_rows;
} RaxH5Writer;

RAXPORT_API int rax_h5_close(RaxH5Writer *writer, char *error, int error_len);

static void set_error(char *error, int error_len, const char *message)
{
    if (error != NULL && error_len > 0) {
        snprintf(error, (size_t)error_len, "%s", message);
    }
}

static hid_t create_fixed_utf8_string_type(size_t size)
{
    hid_t type = H5Tcopy(H5T_C_S1);
    if (type < 0) {
        return -1;
    }
    if (H5Tset_size(type, size) < 0) {
        H5Tclose(type);
        return -1;
    }
    if (H5Tset_cset(type, H5T_CSET_UTF8) < 0) {
        H5Tclose(type);
        return -1;
    }
    if (H5Tset_strpad(type, H5T_STR_NULLTERM) < 0) {
        H5Tclose(type);
        return -1;
    }
    return type;
}

static hid_t create_dataset(hid_t group, const char *name, hid_t type, hsize_t chunk)
{
    hsize_t dims[1] = {0};
    hsize_t maxdims[1] = {H5S_UNLIMITED};
    hsize_t chunks[1] = {chunk};
    hid_t space = H5Screate_simple(1, dims, maxdims);
    hid_t plist = H5Pcreate(H5P_DATASET_CREATE);
    hid_t dataset = -1;

    if (space < 0 || plist < 0) {
        goto done;
    }
    if (H5Pset_chunk(plist, 1, chunks) < 0) {
        goto done;
    }
    if (H5Pset_shuffle(plist) < 0) {
        goto done;
    }
    if (H5Pset_deflate(plist, 6) < 0) {
        goto done;
    }

    dataset = H5Dcreate2(group, name, type, space, H5P_DEFAULT, plist, H5P_DEFAULT);

done:
    if (plist >= 0) {
        H5Pclose(plist);
    }
    if (space >= 0) {
        H5Sclose(space);
    }
    return dataset;
}

static int append_dataset(hid_t dataset, hid_t mem_type, hsize_t offset, hsize_t count, const void *data)
{
    if (count == 0) {
        return RAX_OK;
    }

    hsize_t new_size[1] = {offset + count};
    hsize_t start[1] = {offset};
    hsize_t extent[1] = {count};
    hid_t file_space = -1;
    hid_t mem_space = -1;
    int rc = RAX_FAIL;

    if (H5Dset_extent(dataset, new_size) < 0) {
        goto done;
    }
    file_space = H5Dget_space(dataset);
    mem_space = H5Screate_simple(1, extent, NULL);
    if (file_space < 0 || mem_space < 0) {
        goto done;
    }
    if (H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, NULL, extent, NULL) < 0) {
        goto done;
    }
    if (H5Dwrite(dataset, mem_type, mem_space, file_space, H5P_DEFAULT, data) < 0) {
        goto done;
    }

    rc = RAX_OK;

done:
    if (mem_space >= 0) {
        H5Sclose(mem_space);
    }
    if (file_space >= 0) {
        H5Sclose(file_space);
    }
    return rc;
}


static int append_zero_dataset(hid_t dataset, hid_t mem_type, size_t element_size, hsize_t offset, hsize_t count)
{
    if (count == 0) {
        return RAX_OK;
    }

    const hsize_t max_chunk = PEAK_CHUNK;
    hsize_t new_size[1] = {offset + count};
    hsize_t written = 0;
    void *zeros = NULL;
    int rc = RAX_FAIL;

    if (H5Dset_extent(dataset, new_size) < 0) {
        return RAX_FAIL;
    }

    zeros = calloc((size_t)max_chunk, element_size);
    if (zeros == NULL) {
        return RAX_FAIL;
    }

    while (written < count) {
        hsize_t chunk = count - written;
        hsize_t start[1] = {offset + written};
        hsize_t extent[1];
        hid_t file_space = -1;
        hid_t mem_space = -1;
        if (chunk > max_chunk) {
            chunk = max_chunk;
        }
        extent[0] = chunk;
        file_space = H5Dget_space(dataset);
        mem_space = H5Screate_simple(1, extent, NULL);
        if (file_space < 0 || mem_space < 0 ||
            H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, NULL, extent, NULL) < 0 ||
            H5Dwrite(dataset, mem_type, mem_space, file_space, H5P_DEFAULT, zeros) < 0) {
            if (mem_space >= 0) {
                H5Sclose(mem_space);
            }
            if (file_space >= 0) {
                H5Sclose(file_space);
            }
            goto done;
        }
        H5Sclose(mem_space);
        H5Sclose(file_space);
        written += chunk;
    }

    rc = RAX_OK;

done:
    free(zeros);
    return rc;
}

static int append_dataset_or_zero(hid_t dataset, hid_t mem_type, size_t element_size, hsize_t offset, hsize_t count, const void *data)
{
    if (data != NULL) {
        return append_dataset(dataset, mem_type, offset, count, data);
    }

    return append_zero_dataset(dataset, mem_type, element_size, offset, count);
}

static int append_strings(hid_t dataset, hsize_t offset, hsize_t count, const char **data, size_t string_size)
{
    hid_t type = -1;
    char *buffer = NULL;
    int rc = RAX_FAIL;
    if (count == 0) {
        return RAX_OK;
    }
    type = create_fixed_utf8_string_type(string_size);
    if (type < 0) {
        return RAX_FAIL;
    }
    buffer = (char *)calloc((size_t)count, string_size);
    if (buffer == NULL) {
        goto done;
    }
    for (hsize_t i = 0; i < count; i++) {
        const char *value = data != NULL && data[i] != NULL ? data[i] : "";
        size_t length = strnlen(value, string_size - 1);
        memcpy(buffer + ((size_t)i * string_size), value, length);
    }

    rc = append_dataset(dataset, type, offset, count, buffer);

done:
    free(buffer);
    H5Tclose(type);
    return rc;
}

static int write_int_attr(hid_t object, const char *name, int value)
{
    hid_t space = H5Screate(H5S_SCALAR);
    hid_t attr = -1;
    int rc = RAX_FAIL;
    if (space < 0) {
        return RAX_FAIL;
    }
    attr = H5Acreate2(object, name, H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT);
    if (attr >= 0 && H5Awrite(attr, H5T_NATIVE_INT, &value) >= 0) {
        rc = RAX_OK;
    }
    if (attr >= 0) {
        H5Aclose(attr);
    }
    H5Sclose(space);
    return rc;
}

static int write_string_attr(hid_t object, const char *name, const char *value)
{
    const char *safe_value = value == NULL ? "" : value;
    hid_t space = H5Screate(H5S_SCALAR);
    hid_t type = H5Tcopy(H5T_C_S1);
    hid_t attr = -1;
    int rc = RAX_FAIL;
    if (space < 0 || type < 0) {
        goto done;
    }
    if (H5Tset_size(type, strlen(safe_value) + 1) < 0) {
        goto done;
    }
    if (H5Tset_cset(type, H5T_CSET_UTF8) < 0) {
        goto done;
    }
    attr = H5Acreate2(object, name, type, space, H5P_DEFAULT, H5P_DEFAULT);
    if (attr >= 0 && H5Awrite(attr, type, safe_value) >= 0) {
        rc = RAX_OK;
    }

done:
    if (attr >= 0) {
        H5Aclose(attr);
    }
    if (type >= 0) {
        H5Tclose(type);
    }
    if (space >= 0) {
        H5Sclose(space);
    }
    return rc;
}

static int create_all_datasets(RaxH5Writer *writer)
{
    hid_t scan_filter_string_type = create_fixed_utf8_string_type(SCAN_FILTER_STRING_SIZE);
    hid_t activation_string_type = create_fixed_utf8_string_type(ACTIVATION_STRING_SIZE);
    int rc = RAX_FAIL;
    if (scan_filter_string_type < 0 || activation_string_type < 0) {
        if (scan_filter_string_type >= 0) {
            H5Tclose(scan_filter_string_type);
        }
        if (activation_string_type >= 0) {
            H5Tclose(activation_string_type);
        }
        return RAX_FAIL;
    }

    writer->scan_number = create_dataset(writer->scans, "scan_number", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->ms_order = create_dataset(writer->scans, "ms_order", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->retention_time = create_dataset(writer->scans, "retention_time", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->tic = create_dataset(writer->scans, "tic", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->scan_filter_id = create_dataset(writer->scans, "scan_filter_id", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->activation_id = create_dataset(writer->scans, "activation_id", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->parent_scan_number = create_dataset(writer->scans, "parent_scan_number", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->reaction_start = create_dataset(writer->scans, "reaction_start", H5T_NATIVE_LLONG, SCAN_CHUNK);
    writer->reaction_count = create_dataset(writer->scans, "reaction_count", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->peak_start = create_dataset(writer->scans, "peak_start", H5T_NATIVE_LLONG, SCAN_CHUNK);
    writer->peak_count = create_dataset(writer->scans, "peak_count", H5T_NATIVE_INT, SCAN_CHUNK);

    writer->peak_mz = create_dataset(writer->peaks, "mz", H5T_NATIVE_DOUBLE, PEAK_CHUNK);
    writer->peak_intensity = create_dataset(writer->peaks, "intensity", H5T_NATIVE_DOUBLE, PEAK_CHUNK);
    writer->peak_resolution = create_dataset(writer->peaks, "resolution", H5T_NATIVE_DOUBLE, PEAK_CHUNK);
    writer->peak_baseline = create_dataset(writer->peaks, "baseline", H5T_NATIVE_DOUBLE, PEAK_CHUNK);
    writer->peak_noise = create_dataset(writer->peaks, "noise", H5T_NATIVE_DOUBLE, PEAK_CHUNK);
    writer->peak_charge = create_dataset(writer->peaks, "charge", H5T_NATIVE_INT, PEAK_CHUNK);
    writer->peak_mobility_trace_start = create_dataset(writer->peaks, "mobility_trace_start", H5T_NATIVE_LLONG, PEAK_CHUNK);
    writer->peak_mobility_trace_count = create_dataset(writer->peaks, "mobility_trace_count", H5T_NATIVE_INT, PEAK_CHUNK);

    writer->trace_one_over_k0_index = create_dataset(writer->peak_mobility_traces, "one_over_k0_index", H5T_NATIVE_INT, PEAK_CHUNK);
    writer->trace_intensity = create_dataset(writer->peak_mobility_traces, "intensity", H5T_NATIVE_FLOAT, PEAK_CHUNK);

    writer->reaction_precursor_mass = create_dataset(writer->reactions, "precursor_mass", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_isolation_width = create_dataset(writer->reactions, "isolation_width", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_charge_state = create_dataset(writer->reactions, "charge_state", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->reaction_collision_energy = create_dataset(writer->reactions, "collision_energy", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_collision_energy_valid = create_dataset(writer->reactions, "collision_energy_valid", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->reaction_activation_type_id = create_dataset(writer->reactions, "activation_type_id", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->reaction_multiple_activation = create_dataset(writer->reactions, "multiple_activation", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->reaction_precursor_range_valid = create_dataset(writer->reactions, "precursor_range_valid", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->reaction_first_precursor_mass = create_dataset(writer->reactions, "first_precursor_mass", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_last_precursor_mass = create_dataset(writer->reactions, "last_precursor_mass", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_isolation_width_offset = create_dataset(writer->reactions, "isolation_width_offset", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_one_over_k0_begin = create_dataset(writer->reactions, "one_over_k0_begin", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_one_over_k0_end = create_dataset(writer->reactions, "one_over_k0_end", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->reaction_candidate_start = create_dataset(writer->reactions, "candidate_start", H5T_NATIVE_LLONG, SCAN_CHUNK);
    writer->reaction_candidate_count = create_dataset(writer->reactions, "candidate_count", H5T_NATIVE_INT, SCAN_CHUNK);

    writer->candidate_charge = create_dataset(writer->candidates, "charge", H5T_NATIVE_INT, SCAN_CHUNK);
    writer->candidate_mz = create_dataset(writer->candidates, "mz", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->candidate_intensity = create_dataset(writer->candidates, "intensity", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->candidate_one_over_k0 = create_dataset(writer->candidates, "one_over_k0", H5T_NATIVE_DOUBLE, SCAN_CHUNK);
    writer->scan_filter_value = create_dataset(writer->string_tables, "scan_filter", scan_filter_string_type, SCAN_CHUNK);
    writer->activation_value = create_dataset(writer->string_tables, "activation", activation_string_type, SCAN_CHUNK);
    writer->reaction_activation_type_value = create_dataset(writer->string_tables, "reaction_activation_type", activation_string_type, SCAN_CHUNK);

    if (writer->scan_number >= 0 && writer->ms_order >= 0 && writer->retention_time >= 0 &&
        writer->tic >= 0 && writer->scan_filter_id >= 0 && writer->activation_id >= 0 &&
        writer->parent_scan_number >= 0 && writer->reaction_start >= 0 &&
        writer->reaction_count >= 0 && writer->peak_start >= 0 && writer->peak_count >= 0 &&
        writer->peak_mz >= 0 && writer->peak_intensity >= 0 && writer->peak_resolution >= 0 &&
        writer->peak_baseline >= 0 && writer->peak_noise >= 0 && writer->peak_charge >= 0 &&
        writer->peak_mobility_trace_start >= 0 && writer->peak_mobility_trace_count >= 0 &&
        writer->trace_one_over_k0_index >= 0 && writer->trace_intensity >= 0 &&
        writer->reaction_precursor_mass >= 0 && writer->reaction_isolation_width >= 0 &&
        writer->reaction_charge_state >= 0 && writer->reaction_collision_energy >= 0 &&
        writer->reaction_collision_energy_valid >= 0 && writer->reaction_activation_type_id >= 0 &&
        writer->reaction_multiple_activation >= 0 && writer->reaction_precursor_range_valid >= 0 &&
        writer->reaction_first_precursor_mass >= 0 && writer->reaction_last_precursor_mass >= 0 &&
        writer->reaction_isolation_width_offset >= 0 && writer->reaction_one_over_k0_begin >= 0 &&
        writer->reaction_one_over_k0_end >= 0 && writer->reaction_candidate_start >= 0 &&
        writer->reaction_candidate_count >= 0 && writer->candidate_charge >= 0 && writer->candidate_mz >= 0 &&
        writer->candidate_intensity >= 0 && writer->candidate_one_over_k0 >= 0 &&
        writer->scan_filter_value >= 0 && writer->activation_value >= 0 &&
        writer->reaction_activation_type_value >= 0) {
        rc = RAX_OK;
    }

    H5Tclose(scan_filter_string_type);
    H5Tclose(activation_string_type);
    return rc;
}

RAXPORT_API int rax_h5_create(const char *path, const char *source_raw_file, const char *instrument_model,
                  const char *raxport_version, RaxH5Writer **out_writer, char *error, int error_len)
{
    RaxH5Writer *writer = NULL;
    if (out_writer == NULL) {
        set_error(error, error_len, "Output writer pointer is null.");
        return RAX_FAIL;
    }
    *out_writer = NULL;
    writer = (RaxH5Writer *)calloc(1, sizeof(RaxH5Writer));
    if (writer == NULL) {
        set_error(error, error_len, "Unable to allocate HDF5 writer.");
        return RAX_FAIL;
    }

    writer->file = H5Fcreate(path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    writer->scans = writer->peaks = writer->peak_mobility_traces = writer->reactions = writer->candidates = writer->string_tables = -1;
    if (writer->file < 0) {
        set_error(error, error_len, "Unable to create HDF5 file.");
        free(writer);
        return RAX_FAIL;
    }

    writer->scans = H5Gcreate2(writer->file, "/scans", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    writer->peaks = H5Gcreate2(writer->file, "/peaks", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    writer->peak_mobility_traces = H5Gcreate2(writer->file, "/peak_mobility_traces", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    writer->reactions = H5Gcreate2(writer->file, "/reactions", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    writer->candidates = H5Gcreate2(writer->file, "/precursor_candidates", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    writer->string_tables = H5Gcreate2(writer->file, "/string_tables", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (writer->scans < 0 || writer->peaks < 0 || writer->peak_mobility_traces < 0 ||
        writer->reactions < 0 || writer->candidates < 0 || writer->string_tables < 0) {
        set_error(error, error_len, "Unable to create HDF5 groups.");
        rax_h5_close(writer, error, error_len);
        return RAX_FAIL;
    }

    if (write_int_attr(writer->file, "schema_version", 5) != RAX_OK ||
        write_string_attr(writer->file, "raxport_version", raxport_version) != RAX_OK ||
        write_string_attr(writer->file, "source_raw_file", source_raw_file) != RAX_OK ||
        write_string_attr(writer->file, "instrument_model", instrument_model) != RAX_OK) {
        set_error(error, error_len, "Unable to write HDF5 root attributes.");
        rax_h5_close(writer, error, error_len);
        return RAX_FAIL;
    }

    if (create_all_datasets(writer) != RAX_OK) {
        set_error(error, error_len, "Unable to create HDF5 datasets.");
        rax_h5_close(writer, error, error_len);
        return RAX_FAIL;
    }

    *out_writer = writer;
    return RAX_OK;
}

RAXPORT_API int rax_h5_append(RaxH5Writer *writer,
                  int scan_count,
                  const int *scan_number,
                  const int *ms_order,
                  const double *retention_time,
                  const double *tic,
                  const int *scan_filter_id,
                  const int *activation_id,
                  const int *parent_scan_number,
                  const int64_t *reaction_start,
                  const int *reaction_count,
                  const int64_t *peak_start,
                  const int *peak_count,
                  int peak_total,
                  const double *peak_mz,
                  const double *peak_intensity,
                  const double *peak_resolution,
                  const double *peak_baseline,
                  const double *peak_noise,
                  const int *peak_charge,
                  const int64_t *peak_mobility_trace_start,
                  const int *peak_mobility_trace_count,
                  int mobility_trace_total,
                  const int *mobility_trace_one_over_k0_index,
                  const float *mobility_trace_intensity,
                  int reaction_total,
                  const double *reaction_precursor_mass,
                  const double *reaction_isolation_width,
                  const int *reaction_charge_state,
                  const double *reaction_collision_energy,
                  const int *reaction_collision_energy_valid,
                  const int *reaction_activation_type_id,
                  const int *reaction_multiple_activation,
                  const int *reaction_precursor_range_valid,
                  const double *reaction_first_precursor_mass,
                  const double *reaction_last_precursor_mass,
                  const double *reaction_isolation_width_offset,
                  const double *reaction_one_over_k0_begin,
                  const double *reaction_one_over_k0_end,
                  const int64_t *reaction_candidate_start,
                  const int *reaction_candidate_count,
                  int candidate_total,
                  const int *candidate_charge,
                  const double *candidate_mz,
                  const double *candidate_intensity,
                  const double *candidate_one_over_k0,
                  int new_scan_filter_total,
                  const char **new_scan_filters,
                  int new_activation_total,
                  const char **new_activations,
                  int new_reaction_activation_type_total,
                  const char **new_reaction_activation_types,
                  char *error,
                  int error_len)
{
    hsize_t scan_offset;
    hsize_t peak_offset;
    hsize_t trace_offset;
    hsize_t reaction_offset;
    hsize_t candidate_offset;
    hsize_t scan_filter_offset;
    hsize_t activation_offset;
    hsize_t reaction_activation_type_offset;

    if (writer == NULL) {
        set_error(error, error_len, "HDF5 writer is null.");
        return RAX_FAIL;
    }
    if (scan_count < 0 || peak_total < 0 || mobility_trace_total < 0 || reaction_total < 0 || candidate_total < 0 ||
        new_scan_filter_total < 0 || new_activation_total < 0 || new_reaction_activation_type_total < 0) {
        set_error(error, error_len, "Negative append counts are invalid.");
        return RAX_FAIL;
    }

    scan_offset = writer->scan_rows;
    peak_offset = writer->peak_rows;
    trace_offset = writer->trace_rows;
    reaction_offset = writer->reaction_rows;
    candidate_offset = writer->candidate_rows;
    scan_filter_offset = writer->scan_filter_rows;
    activation_offset = writer->activation_rows;
    reaction_activation_type_offset = writer->reaction_activation_type_rows;

    if (append_dataset(writer->scan_number, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, scan_number) != RAX_OK ||
        append_dataset(writer->ms_order, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, ms_order) != RAX_OK ||
        append_dataset(writer->retention_time, H5T_NATIVE_DOUBLE, scan_offset, (hsize_t)scan_count, retention_time) != RAX_OK ||
        append_dataset(writer->tic, H5T_NATIVE_DOUBLE, scan_offset, (hsize_t)scan_count, tic) != RAX_OK ||
        append_dataset(writer->scan_filter_id, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, scan_filter_id) != RAX_OK ||
        append_dataset(writer->activation_id, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, activation_id) != RAX_OK ||
        append_dataset(writer->parent_scan_number, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, parent_scan_number) != RAX_OK ||
        append_dataset(writer->reaction_start, H5T_NATIVE_LLONG, scan_offset, (hsize_t)scan_count, reaction_start) != RAX_OK ||
        append_dataset(writer->reaction_count, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, reaction_count) != RAX_OK ||
        append_dataset(writer->peak_start, H5T_NATIVE_LLONG, scan_offset, (hsize_t)scan_count, peak_start) != RAX_OK ||
        append_dataset(writer->peak_count, H5T_NATIVE_INT, scan_offset, (hsize_t)scan_count, peak_count) != RAX_OK) {
        set_error(error, error_len, "Unable to append scan datasets.");
        return RAX_FAIL;
    }

    if (append_dataset(writer->peak_mz, H5T_NATIVE_DOUBLE, peak_offset, (hsize_t)peak_total, peak_mz) != RAX_OK ||
        append_dataset(writer->peak_intensity, H5T_NATIVE_DOUBLE, peak_offset, (hsize_t)peak_total, peak_intensity) != RAX_OK ||
        append_dataset_or_zero(writer->peak_resolution, H5T_NATIVE_DOUBLE, sizeof(double), peak_offset, (hsize_t)peak_total, peak_resolution) != RAX_OK ||
        append_dataset_or_zero(writer->peak_baseline, H5T_NATIVE_DOUBLE, sizeof(double), peak_offset, (hsize_t)peak_total, peak_baseline) != RAX_OK ||
        append_dataset_or_zero(writer->peak_noise, H5T_NATIVE_DOUBLE, sizeof(double), peak_offset, (hsize_t)peak_total, peak_noise) != RAX_OK ||
        append_dataset_or_zero(writer->peak_charge, H5T_NATIVE_INT, sizeof(int), peak_offset, (hsize_t)peak_total, peak_charge) != RAX_OK ||
        append_dataset(writer->peak_mobility_trace_start, H5T_NATIVE_LLONG, peak_offset, (hsize_t)peak_total, peak_mobility_trace_start) != RAX_OK ||
        append_dataset(writer->peak_mobility_trace_count, H5T_NATIVE_INT, peak_offset, (hsize_t)peak_total, peak_mobility_trace_count) != RAX_OK) {
        set_error(error, error_len, "Unable to append peak datasets.");
        return RAX_FAIL;
    }

    if (append_dataset(writer->trace_one_over_k0_index, H5T_NATIVE_INT, trace_offset, (hsize_t)mobility_trace_total, mobility_trace_one_over_k0_index) != RAX_OK ||
        append_dataset(writer->trace_intensity, H5T_NATIVE_FLOAT, trace_offset, (hsize_t)mobility_trace_total, mobility_trace_intensity) != RAX_OK) {
        set_error(error, error_len, "Unable to append peak mobility trace datasets.");
        return RAX_FAIL;
    }

    if (append_dataset(writer->reaction_precursor_mass, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_precursor_mass) != RAX_OK ||
        append_dataset(writer->reaction_isolation_width, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_isolation_width) != RAX_OK ||
        append_dataset(writer->reaction_charge_state, H5T_NATIVE_INT, reaction_offset, (hsize_t)reaction_total, reaction_charge_state) != RAX_OK ||
        append_dataset(writer->reaction_collision_energy, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_collision_energy) != RAX_OK ||
        append_dataset(writer->reaction_collision_energy_valid, H5T_NATIVE_INT, reaction_offset, (hsize_t)reaction_total, reaction_collision_energy_valid) != RAX_OK ||
        append_dataset(writer->reaction_activation_type_id, H5T_NATIVE_INT, reaction_offset, (hsize_t)reaction_total, reaction_activation_type_id) != RAX_OK ||
        append_dataset(writer->reaction_multiple_activation, H5T_NATIVE_INT, reaction_offset, (hsize_t)reaction_total, reaction_multiple_activation) != RAX_OK ||
        append_dataset(writer->reaction_precursor_range_valid, H5T_NATIVE_INT, reaction_offset, (hsize_t)reaction_total, reaction_precursor_range_valid) != RAX_OK ||
        append_dataset(writer->reaction_first_precursor_mass, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_first_precursor_mass) != RAX_OK ||
        append_dataset(writer->reaction_last_precursor_mass, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_last_precursor_mass) != RAX_OK ||
        append_dataset(writer->reaction_isolation_width_offset, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_isolation_width_offset) != RAX_OK ||
        append_dataset(writer->reaction_one_over_k0_begin, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_one_over_k0_begin) != RAX_OK ||
        append_dataset(writer->reaction_one_over_k0_end, H5T_NATIVE_DOUBLE, reaction_offset, (hsize_t)reaction_total, reaction_one_over_k0_end) != RAX_OK ||
        append_dataset(writer->reaction_candidate_start, H5T_NATIVE_LLONG, reaction_offset, (hsize_t)reaction_total, reaction_candidate_start) != RAX_OK ||
        append_dataset(writer->reaction_candidate_count, H5T_NATIVE_INT, reaction_offset, (hsize_t)reaction_total, reaction_candidate_count) != RAX_OK) {
        set_error(error, error_len, "Unable to append reaction datasets.");
        return RAX_FAIL;
    }

    if (append_dataset(writer->candidate_charge, H5T_NATIVE_INT, candidate_offset, (hsize_t)candidate_total, candidate_charge) != RAX_OK ||
        append_dataset(writer->candidate_mz, H5T_NATIVE_DOUBLE, candidate_offset, (hsize_t)candidate_total, candidate_mz) != RAX_OK ||
        append_dataset(writer->candidate_intensity, H5T_NATIVE_DOUBLE, candidate_offset, (hsize_t)candidate_total, candidate_intensity) != RAX_OK ||
        append_dataset(writer->candidate_one_over_k0, H5T_NATIVE_DOUBLE, candidate_offset, (hsize_t)candidate_total, candidate_one_over_k0) != RAX_OK) {
        set_error(error, error_len, "Unable to append precursor candidate datasets.");
        return RAX_FAIL;
    }

    if (append_strings(writer->scan_filter_value, scan_filter_offset, (hsize_t)new_scan_filter_total, new_scan_filters, SCAN_FILTER_STRING_SIZE) != RAX_OK ||
        append_strings(writer->activation_value, activation_offset, (hsize_t)new_activation_total, new_activations, ACTIVATION_STRING_SIZE) != RAX_OK ||
        append_strings(writer->reaction_activation_type_value, reaction_activation_type_offset, (hsize_t)new_reaction_activation_type_total, new_reaction_activation_types, ACTIVATION_STRING_SIZE) != RAX_OK) {
        set_error(error, error_len, "Unable to append string table datasets.");
        return RAX_FAIL;
    }

    writer->scan_rows += (hsize_t)scan_count;
    writer->peak_rows += (hsize_t)peak_total;
    writer->trace_rows += (hsize_t)mobility_trace_total;
    writer->reaction_rows += (hsize_t)reaction_total;
    writer->candidate_rows += (hsize_t)candidate_total;
    writer->scan_filter_rows += (hsize_t)new_scan_filter_total;
    writer->activation_rows += (hsize_t)new_activation_total;
    writer->reaction_activation_type_rows += (hsize_t)new_reaction_activation_type_total;

    return RAX_OK;
}

static void close_dataset(hid_t dataset)
{
    if (dataset >= 0) {
        H5Dclose(dataset);
    }
}

RAXPORT_API int rax_h5_close(RaxH5Writer *writer, char *error, int error_len)
{
    int rc = RAX_OK;
    if (writer == NULL) {
        return RAX_OK;
    }

    if (writer->file >= 0 && H5Fflush(writer->file, H5F_SCOPE_GLOBAL) < 0) {
        set_error(error, error_len, "Unable to flush HDF5 file during close.");
        rc = RAX_FAIL;
    }

    close_dataset(writer->scan_number);
    close_dataset(writer->ms_order);
    close_dataset(writer->retention_time);
    close_dataset(writer->tic);
    close_dataset(writer->scan_filter_id);
    close_dataset(writer->activation_id);
    close_dataset(writer->parent_scan_number);
    close_dataset(writer->reaction_start);
    close_dataset(writer->reaction_count);
    close_dataset(writer->peak_start);
    close_dataset(writer->peak_count);
    close_dataset(writer->peak_mz);
    close_dataset(writer->peak_intensity);
    close_dataset(writer->peak_resolution);
    close_dataset(writer->peak_baseline);
    close_dataset(writer->peak_noise);
    close_dataset(writer->peak_charge);
    close_dataset(writer->peak_mobility_trace_start);
    close_dataset(writer->peak_mobility_trace_count);
    close_dataset(writer->trace_one_over_k0_index);
    close_dataset(writer->trace_intensity);
    close_dataset(writer->reaction_precursor_mass);
    close_dataset(writer->reaction_isolation_width);
    close_dataset(writer->reaction_charge_state);
    close_dataset(writer->reaction_collision_energy);
    close_dataset(writer->reaction_collision_energy_valid);
    close_dataset(writer->reaction_activation_type_id);
    close_dataset(writer->reaction_multiple_activation);
    close_dataset(writer->reaction_precursor_range_valid);
    close_dataset(writer->reaction_first_precursor_mass);
    close_dataset(writer->reaction_last_precursor_mass);
    close_dataset(writer->reaction_isolation_width_offset);
    close_dataset(writer->reaction_one_over_k0_begin);
    close_dataset(writer->reaction_one_over_k0_end);
    close_dataset(writer->reaction_candidate_start);
    close_dataset(writer->reaction_candidate_count);
    close_dataset(writer->candidate_charge);
    close_dataset(writer->candidate_mz);
    close_dataset(writer->candidate_intensity);
    close_dataset(writer->candidate_one_over_k0);
    close_dataset(writer->scan_filter_value);
    close_dataset(writer->activation_value);
    close_dataset(writer->reaction_activation_type_value);

    if (writer->scans >= 0) {
        H5Gclose(writer->scans);
    }
    if (writer->peaks >= 0) {
        H5Gclose(writer->peaks);
    }
    if (writer->peak_mobility_traces >= 0) {
        H5Gclose(writer->peak_mobility_traces);
    }
    if (writer->reactions >= 0) {
        H5Gclose(writer->reactions);
    }
    if (writer->candidates >= 0) {
        H5Gclose(writer->candidates);
    }
    if (writer->string_tables >= 0) {
        H5Gclose(writer->string_tables);
    }
    if (writer->file >= 0 && H5Fclose(writer->file) < 0) {
        set_error(error, error_len, "Unable to close HDF5 file.");
        rc = RAX_FAIL;
    }

    free(writer);
    return rc;
}
