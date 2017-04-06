/*

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.BoundsChecking;
import org.joda.time.chrono.ISOChronology;

import com.google.common.base.Charsets;

public class StringFunctionHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctionHelpers.class);

  static final int RADIX = 10;
  static final long MAX_LONG = -Long.MAX_VALUE / RADIX;
  static final int MAX_INT = -Integer.MAX_VALUE / RADIX;

  public static long varTypesToLong(final int start, final int end, DrillBuf buffer){
    if ((end - start) ==0) {
      //empty, not a valid number
      return nfeL(start, end, buffer);
    }

    int readIndex = start;

    boolean negative = buffer.getByte(readIndex) == '-';

    if (negative && ++readIndex == end) {
      //only one single '-'
      return nfeL(start, end, buffer);
    }


    long result = 0;
    int digit;

    while (readIndex < end) {
      digit = Character.digit(buffer.getByte(readIndex++),RADIX);
      //not valid digit.
      if (digit == -1) {
        return nfeL(start, end, buffer);
      }
      //overflow
      if (MAX_LONG > result) {
        return nfeL(start, end, buffer);
      }

      long next = result * RADIX - digit;

      //overflow
      if (next > result) {
        return nfeL(start, end, buffer);
      }
      result = next;
    }
    if (!negative) {
      result = -result;
      //overflow
      if (result < 0) {
        return nfeL(start, end, buffer);
      }
    }

    return result;
  }

  private static int nfeL(int start, int end, DrillBuf buffer){
    byte[] buf = new byte[end - start];
    buffer.getBytes(start, buf, 0, end - start);
    throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
  }

  private static int nfeI(int start, int end, DrillBuf buffer){
    byte[] buf = new byte[end - start];
    buffer.getBytes(start, buf, 0, end - start);
    throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
  }

  public static int varTypesToInt(final int start, final int end, DrillBuf buffer){
    if ((end - start) ==0) {
      //empty, not a valid number
      return nfeI(start, end, buffer);
    }

    int readIndex = start;

    boolean negative = buffer.getByte(readIndex) == '-';

    if (negative && ++readIndex == end) {
      //only one single '-'
      return nfeI(start, end, buffer);
    }

    int result = 0;
    int digit;

    while (readIndex < end) {
      digit = Character.digit(buffer.getByte(readIndex++), RADIX);
      //not valid digit.
      if (digit == -1) {
        return nfeI(start, end, buffer);
      }
      //overflow
      if (MAX_INT > result) {
        return nfeI(start, end, buffer);
      }

      int next = result * RADIX - digit;

      //overflow
      if (next > result) {
        return nfeI(start, end, buffer);
      }
      result = next;
    }
    if (!negative) {
      result = -result;
      //overflow
      if (result < 0) {
        return nfeI(start, end, buffer);
      }
    }

    return result;
  }

  // Assumes Alpha as [A-Za-z0-9]
  // white space is treated as everything else.
  public static void initCap(int start, int end, DrillBuf inBuf, DrillBuf outBuf) {
    boolean capNext = true;
    int out = 0;
    for (int id = start; id < end; id++, out++) {
      byte currentByte = inBuf.getByte(id);

      // 'A - Z' : 0x41 - 0x5A
      // 'a - z' : 0x61 - 0x7A
      // '0-9' : 0x30 - 0x39
      if (capNext) { // curCh is whitespace or first character of word.
        if (currentByte >= 0x30 && currentByte <= 0x39) { // 0-9
          capNext = false;
        } else if (currentByte >= 0x41 && currentByte <= 0x5A) { // A-Z
          capNext = false;
        } else if (currentByte >= 0x61 && currentByte <= 0x7A) { // a-z
          capNext = false;
          currentByte -= 0x20; // Uppercase this character
        }
        // else {} whitespace
      } else { // Inside of a word or white space after end of word.
        if (currentByte >= 0x30 && currentByte <= 0x39) { // 0-9
          // noop
        } else if (currentByte >= 0x41 && currentByte <= 0x5A) { // A-Z
          currentByte -= 0x20; // Lowercase this character
        } else if (currentByte >= 0x61 && currentByte <= 0x7A) { // a-z
          // noop
        } else { // whitespace
          capNext = true;
        }
      }

      outBuf.setByte(out, currentByte);
    } // end of for_loop
  }

  /**
   * Convert a VarCharHolder to a String.
   *
   * VarCharHolders are designed specifically for object reuse and mutability, only use
   * this method when absolutely necessary for interacting with interfaces that must take
   * a String.
   *
   * @param varCharHolder a mutable wrapper object that stores a variable length char array, always in UTF-8
   * @return              String of the bytes interpreted as UTF-8
   */
  public static String getStringFromVarCharHolder(VarCharHolder varCharHolder) {
    return toStringFromUTF8(varCharHolder.start, varCharHolder.end, varCharHolder.buffer);
  }

  /**
   * Convert a NullableVarCharHolder to a String.
   */
  public static String getStringFromVarCharHolder(NullableVarCharHolder varCharHolder) {
    return toStringFromUTF8(varCharHolder.start, varCharHolder.end, varCharHolder.buffer);
  }

  public static String toStringFromUTF8(int start, int end, DrillBuf buffer) {
    byte[] buf = new byte[end - start];
    buffer.getBytes(start, buf, 0, end - start);
    String s = new String(buf, Charsets.UTF_8);
    return s;
  }

  public static String toStringFromUTF16(int start, int end, DrillBuf buffer) {
    byte[] buf = new byte[end - start];
    buffer.getBytes(start, buf, 0, end - start);
    return new String(buf, Charsets.UTF_16);
  }

  private static final ISOChronology CHRONOLOGY = org.joda.time.chrono.ISOChronology.getInstanceUTC();

  public static long getDate(DrillBuf buf, int start, int end){
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      buf.checkBytes(start, end);
    }
    int[] dateFields = memGetDate(buf.memoryAddress(), start, end);
    return CHRONOLOGY.getDateTimeMillis(dateFields[0], dateFields[1], dateFields[2], 0);
  }

  /**
   * Takes a string value, specified as a buffer with a start and end and
   * returns true if the value can be read as a date.
   *
   * @param buf
   * @param start
   * @param end
   * @return true iff the string value can be read as a date
   */
  public static boolean isReadableAsDate(DrillBuf buf, int start, int end){
    // Tried looking for a method that would do this check without relying on
    // an exception in the failure case (for better performance). Joda does
    // not appear to provide such a function, so the try/catch block
    // was chosen for compatibility with the getDate() method that actually
    // returns the result of parsing.
    try {
      getDate(buf, start, end);
      // the parsing from the line above succeeded, this was a valid date
      return true;
    } catch(IllegalArgumentException ex) {
      return false;
    }
  }

  /**
   * Calculates target length after left / right functions are applied.
   * Target length calculation logic for left and right functions is the same,
   * they substring string the same way, just from different sides of the string.
   *
   * {@link #calculateSubstringLength(int, int, int, boolean)} is used for calculation.
   * <ul>If given length is positive or equals zero, offset is 1, substring length is given length,
   * useEnd flag is set to true.</ul>
   * <ul>If given length is negative, offset is given length + 1, useEnd flag is set to false.</ul>
   *
   * @param sourceLength source length
   * @param length length
   * @return target length
   */
  public static int calculateStringLeftRightLength(int sourceLength, int length) {
    boolean isNegativeOffset = length < 0;

    int offset = isNegativeOffset ? Math.abs(length) + 1 : 1;
    int substringLength = isNegativeOffset ? -1 : Math.abs(length);

    return calculateSubstringLength(sourceLength, offset, substringLength, !isNegativeOffset);
  }

  /**
   * Calculates target length after substring function will be applied.
   * useEnd flag is used to distinguish between substring(value, offset) and
   * substring(source, offset, length). If useEnd flag is set to false,
   * length to cut is not taken into account during calculation.
   *
   * Calculation rules for substring(value, offset):
   * <ul>If offset is corrupted (less then or equals 0), target length is 0.<ul/>
   * <ul>If source length after offset is less then or equals 0, target length is 0. <ul/>
   * <ul>For all other cases, target length is source length after offset. <ul/>
   *
   * Calculation rules for substring(value, offset, length):
   * <ul>If offset or length is corrupted (less then or equals 0), target length is 0.<ul/>
   * <ul>If source length after offset is less then length to cut, target length is source length after offset.<ul/>
   * <ul>For all other cases, target length is length to cut.<ul/>
   *
   * @param sourceLength source length
   * @param offset offset
   * @param length length to cut
   * @param useEnd if length to cut should be used in calculation
   * @return target length
   */
  public static int calculateSubstringLength(int sourceLength, int offset, int length, boolean useEnd) {
    if (offset <= 0 || (useEnd && length <= 0)) {
      return 0;
    }
    // Substring offset count starts with 1 (not 0), indicating first character in string.
    // That's why when we calculate source length after offset, we need to add + 1 to receive correct result.
    int sourceLengthAfterOffset = Math.max(sourceLength - offset + 1, 0);
    return useEnd && sourceLengthAfterOffset >= length ? length : sourceLengthAfterOffset;
  }

  private static int[] memGetDate(long memoryAddress, int start, int end){
    long index = memoryAddress + start;
    final long endIndex = memoryAddress + end;
    int digit = 0;

    // Stores three fields (year, month, day)
    int[] dateFields = new int[3];
    int dateIndex = 0;
    int value = 0;

    while (dateIndex < 3 && index < endIndex) {
      digit = Character.digit(PlatformDependent.getByte(index++), RADIX);

      if (digit == -1) {
        dateFields[dateIndex++] = value;
        value = 0;
      } else {
        value = (value * 10) + digit;
      }
    }

    if (dateIndex < 3) {
      // If we reached the end of input, we would have not encountered a separator, store the last value
      dateFields[dateIndex++] = value;
    }

    /* Handle two digit years
     * Follow convention as done by Oracle, Postgres
     * If range of two digits is between 70 - 99 then year = 1970 - 1999
     * Else if two digits is between 00 - 69 = 2000 - 2069
     */
    if (dateFields[0] < 100) {
      if (dateFields[0] < 70) {
        dateFields[0] += 2000;
      } else {
        dateFields[0] += 1900;
      }
    }
    return dateFields;
  }
}
