package xsql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/reverted/ex"
)

var (
	jsonPathParts = []string{
		`^`,
		`(\w+)`, // Base column (required)
		`(?:`,
		`(?:->(?:'\w+'|\w+|"\w+"))+`,  // One or more standard segments
		`(?:->>(?:'\w+'|\w+|"\w+"))?`, // Optional final text segment
		`|`,
		`->>(?:'\w+'|\w+|"\w+")`, // OR one final text segment
		`)`,
		`$`,
	}

	jsonPathRegexp = regexp.MustCompile(strings.Join(jsonPathParts, ""))
	resourceRegexp = regexp.MustCompile(`^\w+$`)
	aliasRegex     = regexp.MustCompile(`(?i)^(.*)\s+AS\s+(?:\w+)$`)
	randomRegexp   = regexp.MustCompile(`(?i)^RANDOM\(\)$`)
)

type validatorOpt func(*validator)

func WithPermittedResourcePattern(pattern string) validatorOpt {
	return func(v *validator) {
		v.ResourcePattern = regexp.MustCompile(pattern)
	}
}

func WithPermittedColumnPatternAlias() validatorOpt {
	return func(v *validator) {
		v.ColumnPatterns = append(v.ColumnPatterns, aliasRegex)
	}
}

func WithPermittedColumnPatternJsonPath() validatorOpt {
	return func(v *validator) {
		v.ColumnPatterns = append(v.ColumnPatterns, jsonPathRegexp)
	}
}

func WithPermittedColumnPatternRandom() validatorOpt {
	return func(v *validator) {
		v.ColumnPatterns = append(v.ColumnPatterns, randomRegexp)
	}
}

func WithPermittedColumnPattern(pattern string) validatorOpt {
	return func(v *validator) {
		v.ColumnPatterns = append(v.ColumnPatterns, regexp.MustCompile(pattern))
	}
}

func WithPermittedColumnPatterns(pattern ...string) validatorOpt {
	return func(v *validator) {
		for _, pattern := range pattern {
			v.ColumnPatterns = append(v.ColumnPatterns, regexp.MustCompile(pattern))
		}
	}
}

func NewValidator(logger Logger, opts ...validatorOpt) *validator {

	validator := &validator{
		Logger:          logger,
		ResourcePattern: resourceRegexp,
		ColumnPatterns:  []*regexp.Regexp{},
	}

	for _, opt := range opts {
		opt(validator)
	}

	return validator
}

type validator struct {
	Logger

	ResourcePattern *regexp.Regexp
	ColumnPatterns  []*regexp.Regexp
}

func (v *validator) Validate(cmd ex.Command, cols map[string]string) error {

	if !v.ResourcePattern.MatchString(cmd.Resource) {
		return fmt.Errorf("invalid resource: %s", cmd.Resource)
	}

	for _, column := range cmd.ColumnConfig {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid select column: %s", column)
		}
	}

	for _, column := range cmd.PartitionConfig {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid partition column: %s", column)
		}
	}

	for column := range cmd.Where {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid where column: %s", column)
		}
	}

	for column := range cmd.Values {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid value column: %s", column)
		}
	}

	for _, column := range cmd.GroupConfig {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid group column: %s", column)
		}
	}

	for _, column := range cmd.OrderConfig {
		if !v.isValidOrderConfig(cols, column) {
			return fmt.Errorf("invalid order column: %s", column)
		}
	}

	for _, column := range cmd.OnConflictConfig.Constraint {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid conflict constraint column: %s", column)
		}
	}

	for _, column := range cmd.OnConflictConfig.Update {
		if !v.isValidColumn(cols, column) {
			return fmt.Errorf("invalid conflict update column: %s", column)
		}
	}

	ignore := cmd.OnConflictConfig.Ignore
	if ignore != "" && strings.ToLower(ignore) != "true" {
		if !v.isValidColumn(cols, ignore) {
			return fmt.Errorf("invalid conflict ignore column: %s", ignore)
		}
	}

	return nil
}

func (v *validator) isValidColumn(cols map[string]string, column string) bool {
	return v.isValidColumnWithVisited(cols, column, make(map[string]bool))
}

func (v *validator) isValidColumnWithVisited(cols map[string]string, column string, visited map[string]bool) bool {

	_, ok := cols[column]
	if ok {
		// This matches a base column -> valid
		return true
	}

	if visited[column] {
		// This means we're in a circular reference -> not valid
		return false
	}

	visited[column] = true

	// Check against valid column patterns
	for _, pattern := range v.ColumnPatterns {
		matches := pattern.FindStringSubmatch(column)

		if len(matches) > 0 {
			// If there are capture groups, ensure each is valid; otherwise, just return true
			for _, match := range matches[1:] {
				if match == "" || !v.isValidColumnWithVisited(cols, match, visited) {
					return false
				}
			}
			return true
		}
	}

	return false

}

func (v *validator) isValidOrderConfig(cols map[string]string, column string) bool {

	parts := strings.Fields(column)

	if len(parts) == 0 {
		return false
	}

	valid := v.isValidColumn(cols, parts[0])

	if len(parts) == 1 {
		return valid
	}

	if len(parts) == 2 {
		direction := strings.ToUpper(parts[1])
		return valid && (direction == "ASC" || direction == "DESC")
	}

	return false
}
