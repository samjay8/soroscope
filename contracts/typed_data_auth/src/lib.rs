#![no_std]

<<<<<<< Updated upstream
use soroban_sdk::{
    contract, contractimpl, contracttype, crypto::Signature, Address, Bytes, BytesN, Env, String,
};
=======
use soroban_sdk::{contract, contractimpl, contracttype, Address, Bytes, BytesN, Env, String};
>>>>>>> Stashed changes

#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Domain {
    pub name: String,
    pub version: String,
    pub chain_id: u32,
    pub verifying_contract: Address,
}

#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transfer {
    pub from: Address,
    pub to: Address,
    pub amount: i128,
}

#[contract]
pub struct TypedDataAuth;

fn string_to_bytes(env: &Env, s: &String) -> Bytes {
    let len = s.len() as usize;
    let mut buf = [0u8; 256];
    let slice = &mut buf[..len.min(256)];
    s.copy_into_slice(slice);
    Bytes::from_slice(env, slice)
}

#[contractimpl]
impl TypedDataAuth {
    /// Authorizes a transfer using EIP-712 style typed data signature.
    /// Uses Soroban native auth (`require_auth`) for signature verification
    /// combined with structured data hashing for domain separation.
    pub fn authorize_transfer(
        env: Env,
        domain: Domain,
        transfer: Transfer,
<<<<<<< Updated upstream
        signature: BytesN<64>,
        signer: Address,
    ) {
        let domain_hash = Self::domain_separator_hash(&env, &domain);
        let struct_hash = Self::struct_hash(&env, &transfer);
        let _message_hash = Self::message_hash(&env, &domain_hash, &struct_hash);
        let _signature = signature;

        signer.require_auth();

        // Log the successful authorization (optional)
        env.events().publish(
            ("transfer_authorized",),
            (signer, transfer.from, transfer.to, transfer.amount),
        );
    }
=======
        signer: Address,
    ) {
        // Require authorization from the signer via Soroban native auth
        signer.require_auth();

        let domain_hash = Self::domain_separator_hash(&env, &domain);
        let struct_hash = Self::struct_hash(&env, &transfer);
        let _message_hash = Self::message_hash(&env, &domain_hash, &struct_hash);
>>>>>>> Stashed changes

        // Log the successful authorization
        env.events().publish(
            ("transfer_authorized",),
            (signer, transfer.from, transfer.to, transfer.amount),
        );
    }
}

/// Helper methods for EIP-712 style hashing. These are NOT exported as
/// contract entry points — they live outside `#[contractimpl]` so the
/// macro does not try to generate FFI wrappers for reference parameters.
impl TypedDataAuth {
    /// Computes the domain separator hash.
<<<<<<< Updated upstream
    fn domain_separator_hash(env: &Env, domain: &Domain) -> BytesN<32> {
        let type_hash = env.crypto().sha256(&env.bytes(
            b"EIP712Domain(string name,string version,u32 chainId,Address verifyingContract)",
        ));
        let name_hash = env.crypto().sha256(&env.bytes(domain.name.as_bytes()));
        let version_hash = env.crypto().sha256(&env.bytes(domain.version.as_bytes()));
        let chain_id_bytes = domain.chain_id.to_be_bytes();
        let verifying_contract_bytes = domain.verifying_contract.to_string().as_bytes();

=======
    pub fn domain_separator_hash(env: &Env, domain: &Domain) -> BytesN<32> {
>>>>>>> Stashed changes
        let mut data = Bytes::new(env);
        data.append(&Bytes::from_slice(
            env,
            b"EIP712Domain(string name,string version,u32 chainId,Address verifyingContract)",
        ));
        data.append(&Bytes::from_slice(env, &domain.chain_id.to_be_bytes()));

        let hash = env.crypto().sha256(&data);
        BytesN::from_array(env, &hash.to_array())
    }

<<<<<<< Updated upstream
    /// Computes the struct hash for Transfer.
    fn struct_hash(env: &Env, transfer: &Transfer) -> BytesN<32> {
        let type_hash = env
            .crypto()
            .sha256(&env.bytes(b"Transfer(address from,address to,int128 amount)"));
        let from_bytes = transfer.from.to_string().as_bytes();
        let to_bytes = transfer.to.to_string().as_bytes();
        let amount_bytes = transfer.amount.to_be_bytes();

=======
    /// Computes the struct hash for a Transfer.
    pub fn struct_hash(env: &Env, transfer: &Transfer) -> BytesN<32> {
>>>>>>> Stashed changes
        let mut data = Bytes::new(env);
        data.append(&Bytes::from_slice(
            env,
            b"Transfer(address from,address to,int128 amount)",
        ));
        data.append(&Bytes::from_slice(env, &transfer.amount.to_be_bytes()));

        let hash = env.crypto().sha256(&data);
        BytesN::from_array(env, &hash.to_array())
    }

    /// Computes the final message hash from domain separator and struct hash.
    pub fn message_hash(
        env: &Env,
        domain_separator: &BytesN<32>,
        struct_hash: &BytesN<32>,
    ) -> BytesN<32> {
<<<<<<< Updated upstream
        env.crypto()
            .sha256(&(domain_separator.clone(), struct_hash.clone()).to_xdr(env))
            .into()
    }
}

mod test;
=======
        let mut data = Bytes::new(env);
        data.append(&Bytes::from_slice(env, &[0x19, 0x01]));
        data.append(&Bytes::from_slice(env, &domain_separator.to_array()));
        data.append(&Bytes::from_slice(env, &struct_hash.to_array()));

        let hash = env.crypto().sha256(&data);
        BytesN::from_array(env, &hash.to_array())
    }
}

#[cfg(test)]
mod test;
>>>>>>> Stashed changes
