package domain

import (
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf16"
)

const (
	smsGSM7Limit    = 160
	smsUCS2Limit    = 70
	emailMaxBytes   = 100 * 1024
	pushMaxBytes    = 4 * 1024 // APNs and FCM payload limit
	pushTokenMaxLen = 4096
)

// E.164: a plus sign, a non-zero country code digit, at most 15 digits in total.
var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

func ValidateRecipient(c Channel, recipient string) error {
	if strings.TrimSpace(recipient) == "" {
		return errors.New("is required")
	}
	switch c {
	case ChannelSMS:
		if !e164.MatchString(recipient) {
			return errors.New("must be an E.164 phone number, for example +905551234567")
		}
	case ChannelEmail:
		addr, err := mail.ParseAddress(recipient)
		// Reject display-name forms such as "Ayşe <a@b.com>": the provider expects a bare address.
		if err != nil || addr.Address != recipient {
			return errors.New("must be a valid email address")
		}
	case ChannelPush:
		if len(recipient) > pushTokenMaxLen || strings.IndexFunc(recipient, unicode.IsSpace) >= 0 {
			return fmt.Errorf("must be a device token without spaces, at most %d characters", pushTokenMaxLen)
		}
	}
	return nil
}

func ValidateContent(c Channel, content string) error {
	if strings.TrimSpace(content) == "" {
		return errors.New("is required")
	}
	switch c {
	case ChannelSMS:
		s := MeasureSMS(content)
		if s.Length > s.Limit {
			if s.Encoding == EncodingUCS2 {
				return fmt.Errorf("is %d characters; SMS allows %d because it contains characters outside GSM-7 (such as ş, ğ, ı, İ or emoji)", s.Length, s.Limit)
			}
			return fmt.Errorf("is %d characters; SMS allows %d", s.Length, s.Limit)
		}
	case ChannelEmail:
		if len(content) > emailMaxBytes {
			return fmt.Errorf("must be at most %d bytes", emailMaxBytes)
		}
	case ChannelPush:
		if len(content) > pushMaxBytes {
			return fmt.Errorf("must be at most %d bytes", pushMaxBytes)
		}
	}
	return nil
}

type SMSEncoding string

const (
	EncodingGSM7 SMSEncoding = "GSM-7"
	EncodingUCS2 SMSEncoding = "UCS-2"
)

type SMSMeasure struct {
	Encoding SMSEncoding
	Length   int // in the units the limit is counted in
	Limit    int // for a single segment
}

// MeasureSMS reports how long content is for a single SMS segment.
//
// GSM-7 (3GPP TS 23.038) fits 160 characters, but characters from its
// extension table (such as € or {) take two slots. One character outside
// GSM-7 switches the whole message to UCS-2, which fits 70 UTF-16 code
// units; an emoji outside the Basic Multilingual Plane takes two.
func MeasureSMS(content string) SMSMeasure {
	length := 0
	for _, r := range content {
		switch {
		case strings.ContainsRune(gsm7Basic, r):
			length++
		case strings.ContainsRune(gsm7Extension, r):
			length += 2
		default:
			return SMSMeasure{Encoding: EncodingUCS2, Length: len(utf16.Encode([]rune(content))), Limit: smsUCS2Limit}
		}
	}
	return SMSMeasure{Encoding: EncodingGSM7, Length: length, Limit: smsGSM7Limit}
}

// The GSM-7 default alphabet, without the escape code. Of the Turkish letters,
// ö ü Ö Ü Ç are here; ç ş ğ ı İ Ş Ğ are not.
const gsm7Basic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
	"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"

// Characters reached through the escape code: two slots each.
const gsm7Extension = "\f^{}\\[~]|€"
