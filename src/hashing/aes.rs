// S-box for the SubBytes transformation.
const S_BOX: [u8; 256] = [
    0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
    0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
    0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
    0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
    0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
    0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
    0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
    0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
    0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
    0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
    0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
    0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
    0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
    0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
    0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
    0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16,
];

// Inverse S-box for the InvSubBytes transformation.
const INV_S_BOX: [u8; 256] = [
    0x52, 0x09, 0x6a, 0xd5, 0x30, 0x36, 0xa5, 0x38, 0xbf, 0x40, 0xa3, 0x9e, 0x81, 0xf3, 0xd7, 0xfb,
    0x7c, 0xe3, 0x39, 0x82, 0x9b, 0x2f, 0xff, 0x87, 0x34, 0x8e, 0x43, 0x44, 0xc4, 0xde, 0xe9, 0xcb,
    0x54, 0x7b, 0x94, 0x32, 0xa6, 0xc2, 0x23, 0x3d, 0xee, 0x4c, 0x95, 0x0b, 0x42, 0xfa, 0xc3, 0x4e,
    0x08, 0x2e, 0xa1, 0x66, 0x28, 0xd9, 0x24, 0xb2, 0x76, 0x5b, 0xa2, 0x49, 0x6d, 0x8b, 0xd1, 0x25,
    0x72, 0xf8, 0xf6, 0x64, 0x86, 0x68, 0x98, 0x16, 0xd4, 0xa4, 0x5c, 0xcc, 0x5d, 0x65, 0xb6, 0x92,
    0x6c, 0x70, 0x48, 0x50, 0xfd, 0xed, 0xb9, 0xda, 0x5e, 0x15, 0x46, 0x57, 0xa7, 0x8d, 0x9d, 0x84,
    0x90, 0xd8, 0xab, 0x00, 0x8c, 0xbc, 0xd3, 0x0a, 0xf7, 0xe4, 0x58, 0x05, 0xb8, 0xb3, 0x45, 0x06,
    0xd0, 0x2c, 0x1e, 0x8f, 0xca, 0x3f, 0x0f, 0x02, 0xc1, 0xaf, 0xbd, 0x03, 0x01, 0x13, 0x8a, 0x6b,
    0x3a, 0x91, 0x11, 0x41, 0x4f, 0x67, 0xdc, 0xea, 0x97, 0xf2, 0xcf, 0xce, 0xf0, 0xb4, 0xe6, 0x73,
    0x96, 0xac, 0x74, 0x22, 0xe7, 0xad, 0x35, 0x85, 0xe1, 0xf1, 0x76, 0x6d, 0x8c, 0xb1, 0x5c, 0x4f,
    0x9f, 0x2a, 0x2d, 0xc5, 0x18, 0x54, 0x11, 0x56, 0x0f, 0x74, 0x19, 0x93, 0x37, 0x8a, 0xa1, 0x60,
    0x8e, 0xe4, 0x67, 0x36, 0xc3, 0xf5, 0x24, 0xd8, 0x73, 0x4d, 0xa7, 0x7b, 0x08, 0x57, 0x6c, 0x40,
    0x4d, 0x31, 0x33, 0xc7, 0x0e, 0xa8, 0x59, 0x95, 0xe9, 0x17, 0x4c, 0x7e, 0x14, 0x1a, 0x5e, 0xd4,
    0x9f, 0xf0, 0x49, 0x7a, 0x9b, 0x6e, 0x77, 0xd9, 0xf9, 0x3e, 0x12, 0x06, 0x45, 0x8c, 0xb3, 0x94,
    0x90, 0x8e, 0x9b, 0x7a, 0x14, 0x5c, 0x63, 0x8b, 0x80, 0xc5, 0x91, 0x5b, 0x0d, 0x0c, 0xd1, 0x18,
    0x1c, 0x1c, 0x2a, 0x9f, 0xf7, 0x8b, 0x7d, 0x26, 0x9b, 0x84, 0xfe, 0xda, 0xfe, 0xd5, 0x5e, 0x60,
];

// Rcon table for the key expansion routine.
const RCON: [u8; 11] = [
    0x8d, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36,
];

// AES struct to hold the round keys.
pub struct Aes {
    round_keys: [u8; 176],
}

impl Aes {
    /// Creates a new AES instance with a 16-byte key and performs key expansion.
    pub fn new(key: &[u8; 8]) -> Self {
        let mut round_keys = [0u8; 176];
        round_keys[0..8].copy_from_slice(key);
        let mut temp_word = [0u8; 4];

        for i in 4..44 {
            temp_word.copy_from_slice(&round_keys[(i - 1) * 4..i * 4]);

            if i % 4 == 0 {
                // RotWord: Cyclic shift left
                let temp_byte = temp_word[0];
                temp_word[0] = temp_word[1];
                temp_word[1] = temp_word[2];
                temp_word[2] = temp_word[3];
                temp_word[3] = temp_byte;

                // SubWord: S-box substitution
                temp_word[0] = S_BOX[temp_word[0] as usize];
                temp_word[1] = S_BOX[temp_word[1] as usize];
                temp_word[2] = S_BOX[temp_word[2] as usize];
                temp_word[3] = S_BOX[temp_word[3] as usize];

                // Add Rcon: XOR with Rcon
                temp_word[0] ^= RCON[i / 4];
            }

            round_keys[i * 4] = round_keys[(i - 4) * 4] ^ temp_word[0];
            round_keys[i * 4 + 1] = round_keys[(i - 4) * 4 + 1] ^ temp_word[1];
            round_keys[i * 4 + 2] = round_keys[(i - 4) * 4 + 2] ^ temp_word[2];
            round_keys[i * 4 + 3] = round_keys[(i - 4) * 4 + 3] ^ temp_word[3];
        }

        Aes { round_keys }
    }

    /// Encrypts a single 16-byte block.
    pub fn encrypt_block(&self, block: &mut [u8; 16]) {
        let mut state = *block;
        Self::add_round_key(&mut state, &self.round_keys[0..16]);

        for round in 1..10 {
            Self::sub_bytes(&mut state);
            Self::shift_rows(&mut state);
            Self::mix_columns(&mut state);
            Self::add_round_key(&mut state, &self.round_keys[round * 16..round * 16 + 16]);
        }

        Self::sub_bytes(&mut state);
        Self::shift_rows(&mut state);
        Self::add_round_key(&mut state, &self.round_keys[160..176]);

        *block = state;
    }

    /// Decrypts a single 16-byte block.
    pub fn decrypt_block(&self, block: &mut [u8; 16]) {
        let mut state = *block;

        Self::add_round_key(&mut state, &self.round_keys[160..176]);
        Self::inv_shift_rows(&mut state);
        Self::inv_sub_bytes(&mut state);

        for round in (1..10).rev() {
            Self::add_round_key(&mut state, &self.round_keys[round * 16..round * 16 + 16]);
            Self::inv_mix_columns(&mut state);
            Self::inv_shift_rows(&mut state);
            Self::inv_sub_bytes(&mut state);
        }

        Self::add_round_key(&mut state, &self.round_keys[0..16]);

        *block = state;
    }

    /// Applies the AddRoundKey transformation.
    fn add_round_key(state: &mut [u8; 16], round_key: &[u8]) {
        for i in 0..16 {
            state[i] ^= round_key[i];
        }
    }

    /// Applies the SubBytes transformation.
    fn sub_bytes(state: &mut [u8; 16]) {
        for i in 0..16 {
            state[i] = S_BOX[state[i] as usize];
        }
    }

    /// Applies the ShiftRows transformation.
    fn shift_rows(state: &mut [u8; 16]) {
        let mut temp = [0u8; 16];
        temp[0] = state[0];
        temp[4] = state[4];
        temp[8] = state[8];
        temp[12] = state[12];
        temp[1] = state[5];
        temp[5] = state[9];
        temp[9] = state[13];
        temp[13] = state[1];
        temp[2] = state[10];
        temp[6] = state[14];
        temp[10] = state[2];
        temp[14] = state[6];
        temp[3] = state[15];
        temp[7] = state[3];
        temp[11] = state[7];
        temp[15] = state[11];
        state.copy_from_slice(&temp);
    }

    /// Applies the MixColumns transformation.
    fn mix_columns(state: &mut [u8; 16]) {
        for i in 0..4 {
            let s0 = state[i * 4];
            let s1 = state[i * 4 + 1];
            let s2 = state[i * 4 + 2];
            let s3 = state[i * 4 + 3];

            state[i * 4] = Self::gmul(s0, 2) ^ Self::gmul(s1, 3) ^ s2 ^ s3;
            state[i * 4 + 1] = s0 ^ Self::gmul(s1, 2) ^ Self::gmul(s2, 3) ^ s3;
            state[i * 4 + 2] = s0 ^ s1 ^ Self::gmul(s2, 2) ^ Self::gmul(s3, 3);
            state[i * 4 + 3] = Self::gmul(s0, 3) ^ s1 ^ s2 ^ Self::gmul(s3, 2);
        }
    }

    /// Applies the InvSubBytes transformation.
    fn inv_sub_bytes(state: &mut [u8; 16]) {
        for i in 0..16 {
            state[i] = INV_S_BOX[state[i] as usize];
        }
    }

    /// Applies the InvShiftRows transformation.
    fn inv_shift_rows(state: &mut [u8; 16]) {
        let mut temp = [0u8; 16];
        temp[0] = state[0];
        temp[4] = state[4];
        temp[8] = state[8];
        temp[12] = state[12];
        temp[1] = state[13];
        temp[5] = state[1];
        temp[9] = state[5];
        temp[13] = state[9];
        temp[2] = state[10];
        temp[6] = state[2];
        temp[10] = state[6];
        temp[14] = state[14];
        temp[3] = state[7];
        temp[7] = state[11];
        temp[11] = state[15];
        temp[15] = state[3];
        state.copy_from_slice(&temp);
    }

    /// Applies the InvMixColumns transformation.
    fn inv_mix_columns(state: &mut [u8; 16]) {
        for i in 0..4 {
            let s0 = state[i * 4];
            let s1 = state[i * 4 + 1];
            let s2 = state[i * 4 + 2];
            let s3 = state[i * 4 + 3];

            state[i * 4] = Self::gmul(s0, 0x0e)
                ^ Self::gmul(s1, 0x0b)
                ^ Self::gmul(s2, 0x0d)
                ^ Self::gmul(s3, 0x09);
            state[i * 4 + 1] = Self::gmul(s0, 0x09)
                ^ Self::gmul(s1, 0x0e)
                ^ Self::gmul(s2, 0x0b)
                ^ Self::gmul(s3, 0x0d);
            state[i * 4 + 2] = Self::gmul(s0, 0x0d)
                ^ Self::gmul(s1, 0x09)
                ^ Self::gmul(s2, 0x0e)
                ^ Self::gmul(s3, 0x0b);
            state[i * 4 + 3] = Self::gmul(s0, 0x0b)
                ^ Self::gmul(s1, 0x0d)
                ^ Self::gmul(s2, 0x09)
                ^ Self::gmul(s3, 0x0e);
        }
    }

    /// Performs Galois Field (GF(2^8)) multiplication.
    fn gmul(a: u8, b: u8) -> u8 {
        let mut p = 0;
        let mut hi_bit_set;
        let mut b = b;
        for _ in 0..8 {
            if (b & 1) == 1 {
                p ^= a;
            }
            hi_bit_set = (a & 0x80) != 0;
            let a = a << 1;
            if hi_bit_set {
                p ^= 0x1b; // XOR with the irreducible polynomial x^8 + x^4 + x^3 + x + 1
            }
            b >>= 1;
        }
        p
    }
}

// A simple PKCS#7 padding implementation for demonstration.
fn pkcs7_pad(data: &mut Vec<u8>) {
    let padding_len = 16 - (data.len() % 16);
    let padding_byte = padding_len as u8;
    for _ in 0..padding_len {
        data.push(padding_byte);
    }
}

fn pkcs7_unpad(data: &mut Vec<u8>) {
    if let Some(last_byte) = data.last() {
        let padding_len = *last_byte as usize;
        if padding_len > 0 && padding_len <= 16 {
            data.truncate(data.len() - padding_len);
        }
    }
}

// let key = *b"thisisasecretkey"; // 16-byte key for AES-128
//     let mut plaintext = b"Hello, World! I am learning AES."; // Some plaintext
//     let aes = Aes::new(&key);

//     // Convert to Vec<u8> for padding
//     let mut plaintext_vec = plaintext.to_vec();
//     pkcs7_pad(&mut plaintext_vec);

//     let mut ciphertext = plaintext_vec.clone();
//     println!("Original plaintext: {:?}", plaintext);
//     println!("Padded plaintext:   {:?}", plaintext_vec);

//     // Encrypt block by block
//     for chunk in ciphertext.chunks_exact_mut(16) {
//         let mut block: [u8; 16] = chunk.try_into().unwrap();
//         aes.encrypt_block(&mut block);
//         chunk.copy_from_slice(&block);
//     }
//     println!("Encrypted ciphertext: {:?}", ciphertext);

//     // Decrypt block by block
//     let mut decrypted_text = ciphertext.clone();
//     for chunk in decrypted_text.chunks_exact_mut(16) {
//         let mut block: [u8; 16] = chunk.try_into().unwrap();
//         aes.decrypt_block(&mut block);
//         chunk.copy_from_slice(&block);
//     }

//     // Unpad the decrypted data
//     pkcs7_unpad(&mut decrypted_text);
//     println!("Decrypted text (unpadded): {:?}", decrypted_text);

//     // Verify the decrypted text matches the original
//     assert_eq!(&plaintext.to_vec(), &decrypted_text);
// println!("\nVerification successful: The decrypted text matches the original plaintext.");
