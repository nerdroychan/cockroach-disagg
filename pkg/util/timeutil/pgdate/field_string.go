// Code generated by "stringer"; DO NOT EDIT.

package pgdate

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[fieldYear-0]
	_ = x[fieldMonth-1]
	_ = x[fieldDay-2]
	_ = x[fieldEra-3]
	_ = x[fieldHour-4]
	_ = x[fieldMinute-5]
	_ = x[fieldSecond-6]
	_ = x[fieldNanos-7]
	_ = x[fieldMeridian-8]
	_ = x[fieldTZHour-9]
	_ = x[fieldTZMinute-10]
	_ = x[fieldTZSecond-11]
}

const _field_name = "fieldYearfieldMonthfieldDayfieldErafieldHourfieldMinutefieldSecondfieldNanosfieldMeridianfieldTZHourfieldTZMinutefieldTZSecond"

var _field_index = [...]uint8{0, 9, 19, 27, 35, 44, 55, 66, 76, 89, 100, 113, 126}

func (i field) String() string {
	if i >= field(len(_field_index)-1) {
		return "field(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _field_name[_field_index[i]:_field_index[i+1]]
}
