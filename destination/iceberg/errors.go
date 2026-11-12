package iceberg

import (
	"errors"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils/errs"
)

const (
	codeCatalogConfigInvalid   = "iceberg.catalog_config_invalid"
	codeUnsupportedCatalogType = "iceberg.unsupported_catalog_type"
	codePartitionRegexNoMatch  = "iceberg.partition_regex_no_match"
)

func init() { errs.Register("iceberg", classify) }

func classify(err error) *errs.Failure {
	for _, rule := range []struct {
		cause    error
		category errs.Category
		code     string
	}{
		{table.ErrCommitFailed, errs.ConcurrencyConflict, "iceberg.commit_conflict"},
		{rest.ErrCommitStateUnknown, errs.CatalogError, "iceberg.commit_state_unknown"},
		{catalog.ErrNoSuchTable, errs.ObjectNotFound, "iceberg.table_not_found"},
		{catalog.ErrNoSuchNamespace, errs.ObjectNotFound, "iceberg.namespace_not_found"},
		{rest.ErrUnauthorized, errs.AuthFailed, "iceberg.unauthorized"},
		{rest.ErrAuthorizationExpired, errs.AuthFailed, "iceberg.authorization_expired"},
		{rest.ErrOAuthError, errs.AuthFailed, "iceberg.oauth_error"},
		{rest.ErrForbidden, errs.PermissionDenied, "iceberg.forbidden"},
		{rest.ErrServiceUnavailable, errs.NetworkUnreachable, "iceberg.service_unavailable"},
		{rest.ErrServerError, errs.CatalogError, "iceberg.server_error"},
		{rest.ErrBadRequest, errs.ConfigInvalid, "iceberg.bad_request"},
	} {
		if errors.Is(err, rule.cause) {
			return &errs.Failure{Category: rule.category, ClassifiedBy: errs.ClassifiedByVendor, Code: rule.code}
		}
	}
	var vendor interface{ ErrorCode() string }
	if errors.As(err, &vendor) {
		if category, found := objstorage.ServiceCodeCategories[vendor.ErrorCode()]; found {
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: vendor.ErrorCode()}
		}
	}
	var sqlError interface{ SQLState() string }
	if errors.As(err, &sqlError) {
		state := sqlError.SQLState()
		if len(state) >= 2 {
			categories := map[string]errs.Category{"28": errs.AuthFailed, "08": errs.NetworkUnreachable,
				"40": errs.ConcurrencyConflict, "53": errs.ResourceExhausted, "57": errs.ResourceExhausted,
				"3D": errs.ObjectNotFound, "3F": errs.ObjectNotFound, "23": errs.CatalogError,
				"25": errs.CatalogError, "0A": errs.UnsupportedFeature}
			if category, found := categories[state[:2]]; found {
				return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: state}
			}
		}
	}
	if destination.IsWriteFailure(err) {
		return &errs.Failure{Category: errs.DestinationWriteError, ClassifiedBy: errs.ClassifiedByPrecondition}
	}
	return nil
}
