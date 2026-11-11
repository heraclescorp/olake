package iceberg

import (
	"reflect"
	"strings"
	"testing"
)

func TestJavaHeapEnvironment(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
		want []string
	}{
		{"absent", []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes", "OTHER=value"}, []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes", "OTHER=value"}},
		{"append", []string{"OLAKE_JAVA_HEAP_PERCENT=50", "OTHER=value"}, []string{"OLAKE_JAVA_HEAP_PERCENT=50", "OTHER=value", "_JAVA_OPTIONS=-XX:MaxRAMPercentage=50"}},
		{"override inherited", []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes", "OLAKE_JAVA_HEAP_PERCENT=40"}, []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes -XX:MaxRAMPercentage=40", "OLAKE_JAVA_HEAP_PERCENT=40"}},
		{"empty options", []string{"_JAVA_OPTIONS=", "OLAKE_JAVA_HEAP_PERCENT=1"}, []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=1", "OLAKE_JAVA_HEAP_PERCENT=1"}},
		{"upper bound", []string{"OLAKE_JAVA_HEAP_PERCENT=80"}, []string{"OLAKE_JAVA_HEAP_PERCENT=80", "_JAVA_OPTIONS=-XX:MaxRAMPercentage=80"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := append([]string(nil), test.env...)
			got, err := javaHeapEnvironment(test.env)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.want, got) {
				t.Fatalf("want %v, got %v", test.want, got)
			}
			if !reflect.DeepEqual(original, test.env) {
				t.Fatal("mutated parent environment")
			}
		})
	}
}

func TestJavaHeapEnvironmentRejectsInvalidPolicy(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "81", "50.5", "NaN", "Inf", "1e1", " 50", "secret-value"} {
		t.Run(value, func(t *testing.T) {
			_, err := javaHeapEnvironment([]string{"OLAKE_JAVA_HEAP_PERCENT=" + value})
			if err == nil {
				t.Fatal("expected invalid policy error")
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error leaked invalid environment value")
			}
		})
	}
}

func TestJavaHeapPolicyRejectsOptionsThatBypassContainerBudget(t *testing.T) {
	for _, key := range []string{"_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"} {
		for _, option := range []string{"-Xmx1g", "-XX:MaxHeapSize=1073741824", "-XX:MaxRAM=1g", "-XX:-UseContainerSupport"} {
			_, err := javaHeapEnvironment([]string{"OLAKE_JAVA_HEAP_PERCENT=50", key + "=" + option})
			if err == nil {
				t.Fatalf("accepted %s bypassing the per-job budget", key)
			}
		}
	}
}
